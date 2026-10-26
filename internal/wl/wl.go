// Package wl is wl-collector: it counts how often each sender and URL host
// appears in ALLOW and in HOLD/BLOCK verdicts. Items seen often and never in
// a bad verdict become White List candidates (status "pending") that an
// operator approves or rejects on S-03.
package wl

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"xrayplatform/internal/db"
	"xrayplatform/internal/hosts"
	"xrayplatform/internal/model"
)

const Consumer = "wl"

type Item struct {
	Customer string
	Kind     string // url_host or sender
	Value    string
}

type Count struct{ Allow, Bad int64 }

// Collect counts items per verdict. FAIL_OPEN says nothing about the
// message and is ignored, and so are messages already allowed by the White
// List (they are on it already).
func Collect(events []model.Event) map[Item]Count {
	out := map[Item]Count{}
	for _, e := range events {
		if e.Action == model.FailOpen || e.HasStep("whitelist") {
			continue
		}
		items := map[Item]bool{}
		if e.Sender != "" {
			items[Item{e.CustomerID, "sender", e.Sender}] = true
		}
		for _, u := range e.URLs {
			if h := hosts.Normalize(u); h != "" {
				items[Item{e.CustomerID, "url_host", h}] = true
			}
		}
		for it := range items {
			c := out[it]
			if e.Action == model.Allow {
				c.Allow++
			} else {
				c.Bad++
			}
			out[it] = c
		}
	}
	return out
}

type Writer struct {
	pool     *pgxpool.Pool
	minAllow int64
}

func NewWriter(pool *pgxpool.Pool, minAllow int) *Writer {
	return &Writer{pool: pool, minAllow: int64(minAllow)}
}

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
				delete(fresh, e.Key())
			}
		}
		b := &pgx.Batch{}
		for it, c := range Collect(todo) {
			// watching → pending once seen often enough with no bad verdict;
			// pending → watching again on a bad verdict. Decided items keep
			// their status.
			b.Queue(`INSERT INTO wl_candidates AS w (customer_id, kind, value, allow_count, bad_count, status)
				VALUES ($1, $2, $3, $4::bigint, $5::bigint,
					CASE WHEN $4::bigint >= $6::bigint AND $5::bigint = 0 THEN 'pending' ELSE 'watching' END)
				ON CONFLICT (customer_id, kind, value) DO UPDATE SET
					allow_count = w.allow_count + EXCLUDED.allow_count,
					bad_count   = w.bad_count + EXCLUDED.bad_count,
					last_seen   = now(),
					status = CASE
						WHEN w.status IN ('approved', 'rejected') THEN w.status
						WHEN w.bad_count + EXCLUDED.bad_count = 0
						     AND w.allow_count + EXCLUDED.allow_count >= $6::bigint THEN 'pending'
						ELSE 'watching' END`,
				it.Customer, it.Kind, it.Value, c.Allow, c.Bad, w.minAllow)
		}
		if b.Len() == 0 {
			return nil
		}
		return tx.SendBatch(ctx, b).Close()
	})
}
