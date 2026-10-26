// Command kisa-reporter stores BLOCK verdicts in the KISA outbox and sends
// them to KISA with retries (S-11).
package main

import (
	"context"
	"sync"
	"time"

	"xrayplatform/internal/app"
	"xrayplatform/internal/boot"
	"xrayplatform/internal/config"
	"xrayplatform/internal/consumer"
	"xrayplatform/internal/kisa"
)

func main() {
	app.Main("kisa-reporter", func(ctx context.Context, a *app.App) error {
		var env config.Env
		common := config.LoadCommon(&env, ":9104")
		kcfg := config.LoadKafka(&env, "xray-kisa")
		scfg := kisa.SenderConfig{
			URL:         env.Required("KISA_REPORT_URL"),
			APIKey:      env.Str("KISA_API_KEY", ""),
			Timeout:     env.Dur("KISA_TIMEOUT", 5*time.Second),
			Every:       env.Dur("KISA_SEND_EVERY", 2*time.Second),
			BatchSize:   env.Int("KISA_SEND_BATCH", 50),
			MaxAttempts: env.Int("KISA_MAX_ATTEMPTS", 20),
			Lease:       env.Dur("KISA_LEASE", 2*time.Minute),
		}
		if err := env.Err(); err != nil {
			return err
		}
		deps, err := boot.Open(ctx, a, common)
		if err != nil {
			return err
		}
		defer deps.Close()
		a.ServeOps(ctx, common.OpsAddr)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			kisa.NewSender(scfg, deps.Pool, a.Metrics, a.Log).Run(ctx)
		}()
		c := consumer.New(consumer.NewReader(kcfg), kcfg.BatchSize, kcfg.FlushEvery, kisa.Enqueue(deps.Pool), a.Metrics, a.Log)
		err = c.Run(ctx)
		wg.Wait()
		return err
	})
}
