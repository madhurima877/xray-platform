// Package audit is audit-worker: it keeps every verdict in verdict_history
// (S-12), one partition per UTC day, and drops partitions past retention.
package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"xrayplatform/internal/model"
)

const partitionPrefix = "verdict_history_p"

type Writer struct {
	pool  *pgxpool.Pool
	ready map[string]bool // partitions known to exist; used by one goroutine only
}

func NewWriter(pool *pgxpool.Pool) *Writer { return &Writer{pool: pool, ready: map[string]bool{}} }

// PartitionName is the table holding verdicts decided on t's UTC day.
func PartitionName(t time.Time) string { return partitionPrefix + t.UTC().Format("20060102") }

// Handle inserts one batch. The primary key makes a redelivered event a no-op.
func (w *Writer) Handle(ctx context.Context, events []model.Event) error {
	for _, e := range events {
		if err := w.ensurePartition(ctx, e.DecidedAt); err != nil {
			return err
		}
	}
	b := &pgx.Batch{}
	for _, e := range events {
		if e.Reasons == nil {
			e.Reasons = []model.Reason{}
		}
		reasons, err := json.Marshal(e.Reasons)
		if err != nil {
			return err
		}
		urls := e.URLs
		if urls == nil {
			urls = []string{}
		}
		var ai *float32
		if e.AIScore != nil {
			f := float32(*e.AIScore)
			ai = &f
		}
		b.Queue(`INSERT INTO verdict_history
			(customer_id, message_id, decided_at, verdict, level, sender, urls, reasons, ai_score, model_version, latency_ms)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT DO NOTHING`,
			e.CustomerID, e.MessageID, e.DecidedAt, string(e.Action), e.Level, e.Sender, urls,
			reasons, ai, e.ModelVersion, e.LatencyMS)
	}
	return pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
		return tx.SendBatch(ctx, b).Close()
	})
}

func (w *Writer) ensurePartition(ctx context.Context, t time.Time) error {
	name := PartitionName(t)
	if w.ready[name] {
		return nil
	}
	day := t.UTC().Truncate(24 * time.Hour)
	// The name is built from a date only, so it is safe to format into SQL.
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF verdict_history
		FOR VALUES FROM ('%s') TO ('%s')`, name, day.Format(time.RFC3339), day.Add(24*time.Hour).Format(time.RFC3339))
	if _, err := w.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("create partition %s: %w", name, err)
	}
	w.ready[name] = true
	return nil
}

// DropOld drops daily partitions older than keep.
func DropOld(ctx context.Context, pool *pgxpool.Pool, keep time.Duration, log *slog.Logger) (int, error) {
	rows, err := pool.Query(ctx, `SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname = 'verdict_history'`)
	if err != nil {
		return 0, err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	cutoff := time.Now().UTC().Add(-keep).Truncate(24 * time.Hour)
	dropped := 0
	for _, n := range names {
		day, err := time.Parse("20060102", strings.TrimPrefix(n, partitionPrefix))
		if err != nil || !strings.HasPrefix(n, partitionPrefix) || !day.Before(cutoff) {
			continue
		}
		if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{n}.Sanitize()); err != nil {
			return dropped, err
		}
		log.Info("dropped history partition", "partition", n)
		dropped++
	}
	return dropped, nil
}
