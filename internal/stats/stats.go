// Package stats is stats-worker: it rolls verdicts up into per-minute counts
// (S-01 dashboard) and per-day counts by deciding step (S-05).
package stats

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"xrayplatform/internal/db"
	"xrayplatform/internal/model"
)

const Consumer = "stats"

type MinuteKey struct {
	Customer string
	Bucket   time.Time
	Verdict  model.Action
}

type MinuteCount struct {
	N          int64
	LatencySum float64
	LatencyMax float64
}

type ReasonKey struct {
	Customer string
	Day      time.Time
	Step     string
	Action   model.Action
}

// Aggregate counts events per customer, minute and verdict, and per
// customer, day, step and action. A step that fired twice in one message
// (two blacklisted URLs) counts once.
func Aggregate(events []model.Event) (map[MinuteKey]MinuteCount, map[ReasonKey]int64) {
	minutes := map[MinuteKey]MinuteCount{}
	reasons := map[ReasonKey]int64{}
	for _, e := range events {
		t := e.DecidedAt.UTC()
		k := MinuteKey{Customer: e.CustomerID, Bucket: t.Truncate(time.Minute), Verdict: e.Action}
		c := minutes[k]
		c.N++
		c.LatencySum += e.LatencyMS
		c.LatencyMax = max(c.LatencyMax, e.LatencyMS)
		minutes[k] = c

		day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		seen := map[[2]string]bool{}
		for _, r := range e.Reasons {
			id := [2]string{r.Step, string(r.Action)}
			if seen[id] {
				continue
			}
			seen[id] = true
			reasons[ReasonKey{Customer: e.CustomerID, Day: day, Step: r.Step, Action: r.Action}]++
		}
	}
	return minutes, reasons
}

type Writer struct{ pool *pgxpool.Pool }

func NewWriter(pool *pgxpool.Pool) *Writer { return &Writer{pool: pool} }

// Handle stores one batch. Events already counted (Kafka redelivery) are
// skipped in the same transaction.
func (w *Writer) Handle(ctx context.Context, events []model.Event) error {
	return pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
		keys := make([]string, len(events))
		for i, e := range events {
			keys[i] = e.Key()
		}
		fresh, err := db.MarkNew(ctx, tx, Consumer, keys)
		if err != nil {
			return err
		}
		todo := make([]model.Event, 0, len(fresh))
		for _, e := range events {
			if fresh[e.Key()] {
				todo = append(todo, e)
				delete(fresh, e.Key()) // the same key twice in one batch counts once
			}
		}
		minutes, reasons := Aggregate(todo)
		b := &pgx.Batch{}
		for k, c := range minutes {
			b.Queue(`INSERT INTO stats_minute (customer_id, bucket, verdict, count, latency_sum_ms, latency_max_ms)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (customer_id, bucket, verdict) DO UPDATE SET
					count = stats_minute.count + EXCLUDED.count,
					latency_sum_ms = stats_minute.latency_sum_ms + EXCLUDED.latency_sum_ms,
					latency_max_ms = GREATEST(stats_minute.latency_max_ms, EXCLUDED.latency_max_ms)`,
				k.Customer, k.Bucket, string(k.Verdict), c.N, c.LatencySum, c.LatencyMax)
		}
		for k, n := range reasons {
			b.Queue(`INSERT INTO stats_reason_daily (customer_id, day, step, action, count)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (customer_id, day, step, action) DO UPDATE SET count = stats_reason_daily.count + EXCLUDED.count`,
				k.Customer, k.Day, k.Step, string(k.Action), n)
		}
		if b.Len() == 0 {
			return nil
		}
		return tx.SendBatch(ctx, b).Close()
	})
}
