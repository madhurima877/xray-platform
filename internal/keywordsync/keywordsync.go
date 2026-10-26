// Package keywordsync is keyword-sync: it pushes the X-Ray keyword list to
// the relay whenever an operator changed it (keyword_state.version moved
// past pushed_version) and once a day in full, and keeps the Redis keyword
// set in step as a safety net for admin-api.
package keywordsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"xrayplatform/internal/cache"
)

type Syncer struct {
	url       string
	token     string
	fullEvery time.Duration
	client    *http.Client
	pool      *pgxpool.Pool
	cache     *cache.Cache
	now       func() time.Time
}

func New(url, token string, fullEvery time.Duration, client *http.Client, pool *pgxpool.Pool, c *cache.Cache) *Syncer {
	return &Syncer{url: url, token: token, fullEvery: fullEvery, client: client, pool: pool, cache: c, now: time.Now}
}

// Due reports whether the relay needs a push.
func Due(version, pushed int64, pushedAt *time.Time, now time.Time, fullEvery time.Duration) bool {
	return version != pushed || pushedAt == nil || now.Sub(*pushedAt) >= fullEvery
}

// Run is one job run. It returns the number of keywords pushed (0 when the
// relay was already up to date).
func (s *Syncer) Run(ctx context.Context) (int, error) {
	if _, err := s.cache.SyncKeywords(ctx); err != nil {
		return 0, fmt.Errorf("refresh redis: %w", err)
	}
	var version, pushed int64
	var pushedAt *time.Time
	if err := s.pool.QueryRow(ctx, `SELECT version, pushed_version, pushed_at FROM keyword_state WHERE id = 1`).
		Scan(&version, &pushed, &pushedAt); err != nil {
		return 0, err
	}
	if !Due(version, pushed, pushedAt, s.now(), s.fullEvery) {
		return 0, nil
	}
	rows, err := s.pool.Query(ctx, `SELECT phrase FROM keywords WHERE active ORDER BY phrase`)
	if err != nil {
		return 0, err
	}
	phrases, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	if phrases == nil {
		phrases = []string{}
	}
	if err := s.push(ctx, version, phrases); err != nil {
		return 0, err
	}
	// Only record the version we pushed: an edit made meanwhile bumps
	// version again and is pushed on the next run.
	if _, err := s.pool.Exec(ctx, `UPDATE keyword_state SET pushed_version = $1, pushed_at = now() WHERE id = 1`,
		version); err != nil {
		return 0, err
	}
	return len(phrases), nil
}

func (s *Syncer) push(ctx context.Context, version int64, phrases []string) error {
	body, err := json.Marshal(map[string]any{"version": version, "keywords": phrases})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("relay keyword push answered %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}
