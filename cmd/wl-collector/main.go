// Command wl-collector turns senders and hosts that only ever appear in
// ALLOW verdicts into White List candidates for S-03.
package main

import (
	"context"
	"time"

	"xrayplatform/internal/app"
	"xrayplatform/internal/boot"
	"xrayplatform/internal/config"
	"xrayplatform/internal/consumer"
	"xrayplatform/internal/db"
	"xrayplatform/internal/wl"
)

func main() {
	app.Main("wl-collector", func(ctx context.Context, a *app.App) error {
		var env config.Env
		common := config.LoadCommon(&env, ":9103")
		kcfg := config.LoadKafka(&env, "xray-wl")
		minAllow := env.Int("WL_MIN_ALLOW", 100)
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

		go func() {
			t := time.NewTicker(time.Hour)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if _, err := db.PruneProcessed(ctx, deps.Pool, wl.Consumer, dedupeKeep); err != nil {
						a.Log.Warn("prune dedupe keys failed", "err", err)
					}
				}
			}
		}()

		w := wl.NewWriter(deps.Pool, minAllow)
		c := consumer.New(consumer.NewReader(kcfg), kcfg.BatchSize, kcfg.FlushEvery, w.Handle, a.Metrics, a.Log)
		return c.Run(ctx)
	})
}
