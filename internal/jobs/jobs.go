// Package jobs runs a scheduled job: once at start, then on a ticker or when
// an operator presses "sync now" on S-14 (a Redis pub/sub message), and
// records every run in sync_runs.
package jobs

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"xrayplatform/internal/app"
	"xrayplatform/internal/db"
)

// TriggerChannel carries job names published by admin-api.
const TriggerChannel = "xray:sync:trigger"

// Names of the scheduled jobs; admin-api accepts only these.
var Names = []string{"kisa-collector", "master-sync", "keyword-sync"}

type Job struct {
	Name    string
	Every   time.Duration
	Timeout time.Duration // per run
	Run     func(ctx context.Context) (items int, err error)
}

// Trigger asks the job's service to run it now.
func Trigger(ctx context.Context, rdb *redis.Client, name string) error {
	return rdb.Publish(ctx, TriggerChannel, name).Err()
}

// Loop runs j until ctx is done. A run that is in progress when ctx is
// cancelled gets to finish (bounded by its timeout).
func Loop(ctx context.Context, j Job, pool *pgxpool.Pool, rdb *redis.Client, m *app.Metrics, log *slog.Logger) {
	log = log.With("job", j.Name)
	triggers := listen(ctx, rdb, j.Name, log)
	t := time.NewTicker(j.Every)
	defer t.Stop()
	for {
		runOnce(j, pool, m, log)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-triggers:
			log.Info("manual trigger")
			t.Reset(j.Every)
		}
	}
}

func runOnce(j Job, pool *pgxpool.Pool, m *app.Metrics, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), j.Timeout)
	defer cancel()
	started := time.Now()
	items, err := j.Run(ctx)
	if err != nil {
		m.Inc("runs_failed")
		log.Warn("run failed", "err", err, "took_ms", time.Since(started).Milliseconds())
	} else {
		m.Inc("runs_ok")
		m.Set("last_items", int64(items))
		m.Set("last_ok_unix", time.Now().Unix())
		log.Info("run ok", "items", items, "took_ms", time.Since(started).Milliseconds())
	}
	if rerr := db.RecordRun(ctx, pool, j.Name, started, items, err); rerr != nil {
		log.Warn("record run failed", "err", rerr)
	}
}

func listen(ctx context.Context, rdb *redis.Client, name string, log *slog.Logger) <-chan struct{} {
	out := make(chan struct{}, 1)
	sub := rdb.Subscribe(ctx, TriggerChannel)
	go func() {
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				if msg.Payload != name {
					continue
				}
				select {
				case out <- struct{}{}:
				default: // a run is already queued
				}
			}
		}
	}()
	log.Info("listening for manual triggers", "channel", TriggerChannel)
	return out
}
