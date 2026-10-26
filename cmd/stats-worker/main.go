// Command stats-worker rolls result events up into per-minute and per-step
// counts for the dashboard (S-01) and statistics screens (S-05).
package main

import (
	"context"
	"time"

	"xrayplatform/internal/app"
	"xrayplatform/internal/boot"
	"xrayplatform/internal/config"
	"xrayplatform/internal/consumer"
	"xrayplatform/internal/db"
	"xrayplatform/internal/stats"
)

func main() {
	app.Main("stats-worker", func(ctx context.Context, a *app.App) error {
		var env config.Env
		common := config.LoadCommon(&env, ":9101")
		kcfg := config.LoadKafka(&env, "xray-stats")
		dedupeKeep := env.Dur("DEDUPE_KEEP", 72*time.Hour)
		if err := env.Err(); err != nil {
			return err
		}
		deps, err := boot.Open(ctx, a, common)
		if err != nil {
			return err
		}
		defer deps.Close()
		a.ServeOps(ctx, common.OpsAddr)

		go every(ctx, time.Hour, func() {
			if n, err := db.PruneProcessed(ctx, deps.Pool, stats.Consumer, dedupeKeep); err != nil {
				a.Log.Warn("prune dedupe keys failed", "err", err)
			} else if n > 0 {
				a.Log.Info("pruned dedupe keys", "rows", n)
			}
		})

		w := stats.NewWriter(deps.Pool)
		c := consumer.New(consumer.NewReader(kcfg), kcfg.BatchSize, kcfg.FlushEvery, w.Handle, a.Metrics, a.Log)
		return c.Run(ctx)
	})
}

func every(ctx context.Context, d time.Duration, fn func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}
