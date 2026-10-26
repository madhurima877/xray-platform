// Command audit-worker stores every result event in verdict_history (S-12)
// and drops daily partitions older than HISTORY_RETENTION.
package main

import (
	"context"
	"time"

	"xrayplatform/internal/app"
	"xrayplatform/internal/audit"
	"xrayplatform/internal/boot"
	"xrayplatform/internal/config"
	"xrayplatform/internal/consumer"
)

func main() {
	app.Main("audit-worker", func(ctx context.Context, a *app.App) error {
		var env config.Env
		common := config.LoadCommon(&env, ":9102")
		kcfg := config.LoadKafka(&env, "xray-audit")
		retention := env.Dur("HISTORY_RETENTION", 90*24*time.Hour)
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
			t := time.NewTicker(6 * time.Hour)
			defer t.Stop()
			for {
				if _, err := audit.DropOld(ctx, deps.Pool, retention, a.Log); err != nil && ctx.Err() == nil {
					a.Log.Warn("drop old partitions failed", "err", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
			}
		}()

		w := audit.NewWriter(deps.Pool)
		c := consumer.New(consumer.NewReader(kcfg), kcfg.BatchSize, kcfg.FlushEvery, w.Handle, a.Metrics, a.Log)
		return c.Run(ctx)
	})
}
