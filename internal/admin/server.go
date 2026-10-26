// Package admin is admin-api: the REST backend of the admin web (S-01 to
// S-14). Every edit is written to PostgreSQL with an audit_log row in the
// same transaction, then pushed to Redis so xray-api sees it within its
// 10 s customer cache.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"xrayplatform/internal/app"
	"xrayplatform/internal/cache"
)

type Config struct {
	Secret     []byte
	TokenTTL   time.Duration
	CORSOrigin string // the admin web's origin; empty = same origin only
}

type Server struct {
	pool     *pgxpool.Pool
	rdb      *redis.Client
	cache    *cache.Cache
	secret   []byte
	tokenTTL time.Duration
	cors     string
	app      *app.App
	metrics  *app.Metrics
	log      *slog.Logger
}

func New(cfg Config, pool *pgxpool.Pool, rdb *redis.Client, c *cache.Cache, a *app.App) *Server {
	return &Server{pool: pool, rdb: rdb, cache: c, secret: cfg.Secret, tokenTTL: cfg.TokenTTL,
		cors: cfg.CORSOrigin, app: a, metrics: a.Metrics, log: a.Log}
}

// Routes lists every endpoint with the least role that may call it.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	s.app.Register(mux)
	mux.Handle("POST /api/login", s.h(s.login))

	route := func(pattern string, min Role, fn handlerFunc) { mux.Handle(pattern, s.h(s.authorize(min, fn))) }
	route("GET /api/me", Viewer, s.me)

	// S-06 users, S-07 permissions
	route("GET /api/users", Admin, s.listUsers)
	route("POST /api/users", Admin, s.createUser)
	route("PATCH /api/users/{id}", Admin, s.updateUser)

	// S-04 inspection level, S-08 master data
	route("GET /api/customers", Viewer, s.listCustomers)
	route("GET /api/customers/{id}", Viewer, s.getCustomer)
	route("PATCH /api/customers/{id}", Operator, s.updateCustomer)

	// S-02 Black List, S-03 White List
	route("GET /api/lists/{list}", Viewer, s.listEntries)
	route("POST /api/lists/{list}", Operator, s.addEntry)
	route("DELETE /api/lists/{list}/{id}", Operator, s.deleteEntry)
	route("GET /api/wl-candidates", Viewer, s.listCandidates)
	route("POST /api/wl-candidates/{decision}", Operator, s.decideCandidate)

	// keywords (pushed to the relay by keyword-sync)
	route("GET /api/keywords", Viewer, s.listKeywords)
	route("POST /api/keywords", Operator, s.addKeyword)
	route("PATCH /api/keywords/{id}", Operator, s.updateKeyword)
	route("DELETE /api/keywords/{id}", Operator, s.deleteKeyword)

	// S-01 dashboard, S-05 statistics, S-12 history
	route("GET /api/stats/summary", Viewer, s.statsSummary)
	route("GET /api/stats/series", Viewer, s.statsSeries)
	route("GET /api/stats/reasons", Viewer, s.statsReasons)
	route("GET /api/history", Viewer, s.history)

	// S-11 KISA reports
	route("GET /api/kisa/reports", Viewer, s.listReports)
	route("POST /api/kisa/reports/{customer}/{message}/retry", Operator, s.retryReport)

	// S-14 sync
	route("GET /api/sync/status", Viewer, s.syncStatus)
	route("GET /api/sync/runs", Viewer, s.syncRuns)
	route("POST /api/sync/{job}", Operator, s.triggerSync)

	route("GET /api/audit", Admin, s.auditLog)
	return s.withCORS(mux)
}

// handlerFunc returns an error instead of writing it; h turns it into JSON.
type handlerFunc func(w http.ResponseWriter, r *http.Request) error

type apiError struct {
	Status  int
	Message string
}

func (e *apiError) Error() string { return e.Message }

func badRequest(format string, args ...any) error {
	return &apiError{http.StatusBadRequest, fmt.Sprintf(format, args...)}
}

func notFound(what string) error { return &apiError{http.StatusNotFound, what + " not found"} }

func (s *Server) h(fn handlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.metrics.Inc("requests")
		err := fn(w, r)
		if err == nil {
			return
		}
		var ae *apiError
		var pe *pgconn.PgError
		switch {
		case errors.As(err, &ae):
		case errors.As(err, &pe) && pe.Code == "23505":
			ae = &apiError{http.StatusConflict, "already exists"}
		case errors.As(err, &pe) && pe.Code == "23503":
			ae = &apiError{http.StatusBadRequest, "refers to something that does not exist"}
		case errors.Is(err, pgx.ErrNoRows):
			ae = notFound("record").(*apiError)
		default:
			s.metrics.Inc("errors_internal")
			s.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
			ae = &apiError{http.StatusInternalServerError, "internal error"}
		}
		writeJSON(w, ae.Status, map[string]string{"error": ae.Message})
	})
}

func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cors != "" && r.Header.Get("Origin") == s.cors {
			w.Header().Set("Access-Control-Allow-Origin", s.cors)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(v)
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return badRequest("bad JSON body: %v", err)
	}
	return nil
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// audit records one change. Inside a transaction a failure rolls the change back.
func (s *Server) audit(ctx context.Context, q execer, u User, action, entity, id string, detail any) error {
	if detail == nil {
		detail = map[string]any{}
	}
	b, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx, `INSERT INTO audit_log (user_id, username, action, entity, entity_id, detail)
		VALUES ($1, $2, $3, $4, $5, $6)`, u.ID, u.Username, action, entity, id, b)
	if err != nil {
		s.log.Warn("audit write failed", "action", action, "err", err)
	}
	return err
}

// refreshed logs a Redis refresh failure. PostgreSQL already holds the
// change and the next scheduled sync copies it over, so the request still
// succeeds.
func (s *Server) refreshed(what string, err error) bool {
	if err != nil {
		s.metrics.Inc("redis_refresh_errors")
		s.log.Warn("redis refresh failed", "what", what, "err", err)
		return false
	}
	return true
}

// filter builds a WHERE clause with numbered placeholders.
type filter struct {
	conds []string
	args  []any
}

func (f *filter) add(cond string, arg any) {
	f.args = append(f.args, arg)
	f.conds = append(f.conds, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(f.args))))
}

func (f *filter) where() string {
	if len(f.conds) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(f.conds, " AND ")
}

// arg appends an argument and returns its placeholder (for LIMIT/OFFSET).
func (f *filter) arg(v any) string {
	f.args = append(f.args, v)
	return "$" + strconv.Itoa(len(f.args))
}

func pageParams(r *http.Request, def, max int) (limit, offset int, err error) {
	limit, offset = def, 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > max {
			return 0, 0, badRequest("limit must be 1–%d", max)
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if offset, err = strconv.Atoi(v); err != nil || offset < 0 {
			return 0, 0, badRequest("offset must be ≥ 0")
		}
	}
	return limit, offset, nil
}

// timeRange reads from/to (RFC 3339); the default is the last 24 hours.
func timeRange(r *http.Request, maxSpan time.Duration) (from, to time.Time, err error) {
	to = time.Now().UTC()
	from = to.Add(-24 * time.Hour)
	q := r.URL.Query()
	if v := q.Get("to"); v != "" {
		if to, err = time.Parse(time.RFC3339, v); err != nil {
			return from, to, badRequest("to must be RFC 3339")
		}
		if q.Get("from") == "" {
			from = to.Add(-24 * time.Hour)
		}
	}
	if v := q.Get("from"); v != "" {
		if from, err = time.Parse(time.RFC3339, v); err != nil {
			return from, to, badRequest("from must be RFC 3339")
		}
	}
	if !from.Before(to) {
		return from, to, badRequest("from must be before to")
	}
	if to.Sub(from) > maxSpan {
		return from, to, badRequest("time range is longer than %s", maxSpan)
	}
	return from.UTC(), to.UTC(), nil
}

func pathID(r *http.Request, name string) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	if err != nil || id <= 0 {
		return 0, badRequest("%s must be a positive number", name)
	}
	return id, nil
}

// collect scans every row into T by column name.
func collect[T any](ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, sql string, args ...any) ([]T, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByName[T])
	if out == nil {
		out = []T{}
	}
	return out, err
}
