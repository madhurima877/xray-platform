package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

// Role decides what an operator may do. Each role includes the ones below it.
type Role string

const (
	Viewer   Role = "viewer"   // read every screen
	Operator Role = "operator" // + edit lists, keywords, levels, retry reports, run syncs
	Admin    Role = "admin"    // + manage users, read the audit log
)

func (r Role) rank() int {
	switch r {
	case Admin:
		return 3
	case Operator:
		return 2
	case Viewer:
		return 1
	}
	return 0
}

func (r Role) Valid() bool { return r.rank() > 0 }

// Allows reports whether r may do what min requires.
func (r Role) Allows(min Role) bool { return r.rank() >= min.rank() }

type User struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	Role     Role   `json:"role"`
}

type claims struct {
	Role Role `json:"role"`
	jwt.RegisteredClaims
}

const issuer = "xray-admin-api"

func issueToken(secret []byte, u User, ttl time.Duration, now time.Time) (string, error) {
	c := claims{
		Role: u.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    issuer,
			Subject:   fmt.Sprint(u.ID),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString(secret)
}

func parseToken(secret []byte, token string) (int64, error) {
	var c claims
	_, err := jwt.ParseWithClaims(token, &c, func(*jwt.Token) (any, error) { return secret, nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithIssuer(issuer), jwt.WithExpirationRequired())
	if err != nil {
		return 0, err
	}
	var id int64
	if _, err := fmt.Sscan(c.Subject, &id); err != nil || id <= 0 {
		return 0, errors.New("bad subject")
	}
	return id, nil
}

// A hash of a random password, compared against when the username does not
// exist, so a login takes as long for unknown users as for known ones.
var dummyHash, _ = bcrypt.GenerateFromPassword([]byte("x-ray-dummy-password"), bcrypt.DefaultCost)

func hashPassword(pw string) (string, error) {
	if len(pw) < 10 {
		return "", badRequest("password must be at least 10 characters")
	}
	if len(pw) > 72 {
		return "", badRequest("password must be at most 72 bytes")
	}
	b, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	return string(b), err
}

type ctxKey struct{}

func userFrom(ctx context.Context) User {
	u, _ := ctx.Value(ctxKey{}).(User)
	return u
}

// authorize checks the bearer token and loads the user, so a deactivated
// user or a changed role takes effect at once rather than when the token
// expires.
func (s *Server) authorize(min Role, next handlerFunc) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request) error {
		h := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(h, "Bearer ")
		if !ok || token == "" {
			return &apiError{http.StatusUnauthorized, "missing bearer token"}
		}
		id, err := parseToken(s.secret, token)
		if err != nil {
			return &apiError{http.StatusUnauthorized, "invalid or expired token"}
		}
		var u User
		var active bool
		err = s.pool.QueryRow(r.Context(), `SELECT id, username, role, active FROM admin_users WHERE id = $1`, id).
			Scan(&u.ID, &u.Username, &u.Role, &active)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && !active) {
			return &apiError{http.StatusUnauthorized, "user is not active"}
		}
		if err != nil {
			return err
		}
		if !u.Role.Allows(min) {
			return &apiError{http.StatusForbidden, "needs role " + string(min)}
		}
		return next(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, u)))
	}
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
	User      User      `json:"user"`
}

const (
	loginMaxFails = 10
	loginWindow   = 15 * time.Minute
)

func (s *Server) login(w http.ResponseWriter, r *http.Request) error {
	var req loginRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" || req.Password == "" {
		return badRequest("username and password are required")
	}
	ctx := r.Context()
	failKey := "xray:admin:loginfail:" + strings.ToLower(req.Username)
	if n, err := s.rdb.Get(ctx, failKey).Int(); err == nil && n >= loginMaxFails {
		return &apiError{http.StatusTooManyRequests, "too many failed logins; try again later"}
	}

	var u User
	var hash string
	var active bool
	err := s.pool.QueryRow(ctx, `SELECT id, username, role, password_hash, active FROM admin_users WHERE username = $1`,
		req.Username).Scan(&u.ID, &u.Username, &u.Role, &hash, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		hash, active = string(dummyHash), false
	} else if err != nil {
		return err
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)) != nil || !active {
		pipe := s.rdb.Pipeline()
		pipe.Incr(ctx, failKey)
		pipe.Expire(ctx, failKey, loginWindow)
		pipe.Exec(ctx)
		s.metrics.Inc("logins_failed")
		return &apiError{http.StatusUnauthorized, "wrong username or password"}
	}
	s.rdb.Del(ctx, failKey)

	now := time.Now()
	token, err := issueToken(s.secret, u, s.tokenTTL, now)
	if err != nil {
		return err
	}
	s.metrics.Inc("logins_ok")
	s.audit(ctx, s.pool, u, "login", "admin_user", fmt.Sprint(u.ID), nil)
	return writeJSON(w, http.StatusOK, loginResponse{Token: token, ExpiresAt: now.Add(s.tokenTTL), User: u})
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) error {
	return writeJSON(w, http.StatusOK, userFrom(r.Context()))
}

// Bootstrap creates the first admin when there are no users yet.
func (s *Server) Bootstrap(ctx context.Context, username, password string) error {
	if username == "" || password == "" {
		return nil
	}
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM admin_users`).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	hash, err := hashPassword(password)
	if err != nil {
		return fmt.Errorf("ADMIN_BOOTSTRAP_PASSWORD: %w", err)
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO admin_users (username, password_hash, role) VALUES ($1, $2, 'admin')
		ON CONFLICT (username) DO NOTHING`, username, hash)
	if err == nil {
		s.log.Info("created first admin user", "username", username)
	}
	return err
}
