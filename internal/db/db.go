// Package db opens the PostgreSQL pool, applies the embedded migrations and
// holds the small helpers several services share.
package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

// migrateLock is the advisory lock key, so services starting together do not
// apply the same migration twice.
const migrateLock = 7_262_001

func Open(ctx context.Context, url string, maxConns int) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = int32(maxConns)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// Wait for PostgreSQL to come up (docker compose starts everything at once).
	var lastErr error
	for i := 0; i < 30; i++ {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = pool.Ping(pctx)
		cancel()
		if lastErr == nil {
			return pool, nil
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	pool.Close()
	return nil, fmt.Errorf("postgres not reachable: %w", lastErr)
}

// Migrate applies every migrations/NNN_*.sql not applied yet, in order, each
// in its own transaction.
func Migrate(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLock); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrateLock)

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		version := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
		var done bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
			version).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sql, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version)
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %s: %w", version, err)
		}
		log.Info("migration applied", "version", version)
	}
	return nil
}

// MarkNew records event keys for consumer and returns the ones not seen
// before. Run it in the same transaction as the write it protects, so a
// redelivered batch is counted once.
func MarkNew(ctx context.Context, tx pgx.Tx, consumer string, keys []string) (map[string]bool, error) {
	rows, err := tx.Query(ctx, `
		INSERT INTO processed_events (consumer, event_key)
		SELECT $1, k FROM unnest($2::text[]) AS k
		ON CONFLICT DO NOTHING
		RETURNING event_key`, consumer, keys)
	if err != nil {
		return nil, err
	}
	fresh := make(map[string]bool, len(keys))
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		fresh[k] = true
	}
	return fresh, rows.Err()
}

// PruneProcessed deletes dedupe keys older than keep. Kafka redelivers only
// recent events, so a few days is plenty.
func PruneProcessed(ctx context.Context, pool *pgxpool.Pool, consumer string, keep time.Duration) (int64, error) {
	tag, err := pool.Exec(ctx, `DELETE FROM processed_events WHERE consumer = $1 AND processed_at < $2`,
		consumer, time.Now().Add(-keep))
	return tag.RowsAffected(), err
}

// RecordRun stores one scheduled job run for the sync screen.
func RecordRun(ctx context.Context, pool *pgxpool.Pool, job string, started time.Time, items int, runErr error) error {
	msg := ""
	if runErr != nil {
		msg = runErr.Error()
	}
	_, err := pool.Exec(ctx, `INSERT INTO sync_runs (job, started_at, ok, items, error) VALUES ($1, $2, $3, $4, $5)`,
		job, started, runErr == nil, items, msg)
	return err
}
