package admin

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

type keywordRow struct {
	ID        int64     `json:"id" db:"id"`
	Phrase    string    `json:"phrase" db:"phrase"`
	Active    bool      `json:"active" db:"active"`
	CreatedBy string    `json:"created_by" db:"created_by"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

const keywordColumns = `id, phrase, active, created_by, created_at, updated_at`

// normalizePhrase lower-cases and collapses spaces, matching how xray-api
// cleans message text before Aho-Corasick.
func normalizePhrase(p string) (string, error) {
	p = strings.ToLower(strings.Join(strings.Fields(p), " "))
	if n := utf8.RuneCountInString(p); n < 2 || n > 100 {
		return "", badRequest("phrase must be 2–100 characters")
	}
	return p, nil
}

func (s *Server) listKeywords(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	items, err := collect[keywordRow](ctx, s.pool, `SELECT `+keywordColumns+` FROM keywords ORDER BY phrase`)
	if err != nil {
		return err
	}
	var version, pushed int64
	var pushedAt *time.Time
	if err := s.pool.QueryRow(ctx, `SELECT version, pushed_version, pushed_at FROM keyword_state WHERE id = 1`).
		Scan(&version, &pushed, &pushedAt); err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]any{"items": items, "version": version,
		"relay_version": pushed, "relay_pushed_at": pushedAt})
}

// changeKeywords runs fn, bumps the version keyword-sync watches and audits,
// all in one transaction, then refreshes Redis.
func (s *Server) changeKeywords(ctx context.Context, action, id string, fn func(tx pgx.Tx) (keywordRow, error)) (keywordRow, bool, error) {
	var out keywordRow
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		if out, err = fn(tx); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE keyword_state SET version = version + 1 WHERE id = 1`); err != nil {
			return err
		}
		if id == "" {
			id = fmt.Sprint(out.ID)
		}
		return s.audit(ctx, tx, userFrom(ctx), action, "keyword", id, map[string]any{"phrase": out.Phrase, "active": out.Active})
	})
	if err != nil {
		return out, false, err
	}
	_, rerr := s.cache.SyncKeywords(ctx)
	return out, s.refreshed("keywords", rerr), nil
}

func oneKeyword(ctx context.Context, tx pgx.Tx, sql string, args ...any) (keywordRow, error) {
	rows, err := collect[keywordRow](ctx, tx, sql, args...)
	if err != nil {
		return keywordRow{}, err
	}
	if len(rows) == 0 {
		return keywordRow{}, notFound("keyword")
	}
	return rows[0], nil
}

func (s *Server) addKeyword(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		Phrase string `json:"phrase"`
	}
	if err := decode(r, &req); err != nil {
		return err
	}
	phrase, err := normalizePhrase(req.Phrase)
	if err != nil {
		return err
	}
	ctx := r.Context()
	out, ok, err := s.changeKeywords(ctx, "add", "", func(tx pgx.Tx) (keywordRow, error) {
		return oneKeyword(ctx, tx, `INSERT INTO keywords (phrase, created_by) VALUES ($1, $2) RETURNING `+keywordColumns,
			phrase, userFrom(ctx).Username)
	})
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusCreated, map[string]any{"keyword": out, "redis_refreshed": ok})
}

func (s *Server) updateKeyword(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r, "id")
	if err != nil {
		return err
	}
	var req struct {
		Active *bool `json:"active"`
	}
	if err := decode(r, &req); err != nil {
		return err
	}
	if req.Active == nil {
		return badRequest("active is required")
	}
	ctx := r.Context()
	out, ok, err := s.changeKeywords(ctx, "update", fmt.Sprint(id), func(tx pgx.Tx) (keywordRow, error) {
		return oneKeyword(ctx, tx, `UPDATE keywords SET active = $2, updated_at = now() WHERE id = $1 RETURNING `+keywordColumns,
			id, *req.Active)
	})
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]any{"keyword": out, "redis_refreshed": ok})
}

func (s *Server) deleteKeyword(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r, "id")
	if err != nil {
		return err
	}
	ctx := r.Context()
	out, ok, err := s.changeKeywords(ctx, "delete", fmt.Sprint(id), func(tx pgx.Tx) (keywordRow, error) {
		return oneKeyword(ctx, tx, `DELETE FROM keywords WHERE id = $1 RETURNING `+keywordColumns, id)
	})
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]any{"deleted": out, "redis_refreshed": ok})
}
