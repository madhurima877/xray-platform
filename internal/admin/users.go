package admin

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type userRow struct {
	ID        int64     `json:"id" db:"id"`
	Username  string    `json:"username" db:"username"`
	Role      Role      `json:"role" db:"role"`
	Active    bool      `json:"active" db:"active"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) error {
	users, err := collect[userRow](r.Context(), s.pool,
		`SELECT id, username, role, active, created_at, updated_at FROM admin_users ORDER BY username`)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]any{"items": users})
}

type createUserRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Role     Role   `json:"role"`
}

func (s *Server) createUser(w http.ResponseWriter, r *http.Request) error {
	var req createUserRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	req.Username = strings.TrimSpace(req.Username)
	if len(req.Username) < 3 || len(req.Username) > 64 {
		return badRequest("username must be 3–64 characters")
	}
	if !req.Role.Valid() {
		return badRequest("role must be admin, operator or viewer")
	}
	hash, err := hashPassword(req.Password)
	if err != nil {
		return err
	}
	ctx := r.Context()
	var out userRow
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `INSERT INTO admin_users (username, password_hash, role) VALUES ($1, $2, $3)
			RETURNING id, username, role, active, created_at, updated_at`, req.Username, hash, req.Role).
			Scan(&out.ID, &out.Username, &out.Role, &out.Active, &out.CreatedAt, &out.UpdatedAt)
		if err != nil {
			return err
		}
		return s.audit(ctx, tx, userFrom(ctx), "create", "admin_user", fmt.Sprint(out.ID),
			map[string]any{"username": out.Username, "role": out.Role})
	})
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusCreated, out)
}

type updateUserRequest struct {
	Role     *Role   `json:"role"`
	Active   *bool   `json:"active"`
	Password *string `json:"password"`
}

func (s *Server) updateUser(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r, "id")
	if err != nil {
		return err
	}
	var req updateUserRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	me := userFrom(r.Context())
	// An admin cannot lock themselves out.
	if id == me.ID && ((req.Active != nil && !*req.Active) || (req.Role != nil && *req.Role != Admin)) {
		return badRequest("you cannot deactivate or demote yourself")
	}
	if req.Role != nil && !req.Role.Valid() {
		return badRequest("role must be admin, operator or viewer")
	}
	var hash *string
	if req.Password != nil {
		h, err := hashPassword(*req.Password)
		if err != nil {
			return err
		}
		hash = &h
	}
	ctx := r.Context()
	var out userRow
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `UPDATE admin_users SET
				role = COALESCE($2, role),
				active = COALESCE($3, active),
				password_hash = COALESCE($4, password_hash),
				updated_at = now()
			WHERE id = $1
			RETURNING id, username, role, active, created_at, updated_at`, id, req.Role, req.Active, hash).
			Scan(&out.ID, &out.Username, &out.Role, &out.Active, &out.CreatedAt, &out.UpdatedAt)
		if err != nil {
			return err
		}
		detail := map[string]any{"role": req.Role, "active": req.Active, "password_changed": hash != nil}
		return s.audit(ctx, tx, me, "update", "admin_user", fmt.Sprint(id), detail)
	})
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, out)
}
