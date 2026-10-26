// Command kisa-collector pulls KISA's bad-URL list into the global Black
// List and refreshes Redis.
package main

import (
	"context"
	"net/http"
	"time"

	"xrayplatform/internal/app"
	"xrayplatform/internal/boot"
	"xrayplatform/internal/config"
	"xrayplatform/internal/jobs"
	"xrayplatform/internal/kisa"
)

func main() {
	app.Main("kisa-collector", func(ctx context.Context, a *app.App) error {
		var env config.Env
		common := config.LoadCommon(&env, ":9105")
		url := env.Required("KISA_LIST_URL")
		apiKey := env.Str("KISA_API_KEY", "")
		every := env.Dur("EVERY", time.Hour)
		timeout := env.Dur("RUN_TIMEOUT", 5*time.Minute)
		if err := env.Err(); err != nil {
			return err
		}
		deps, err := boot.Open(ctx, a, common)
		if err != nil {
			return err
		}
		defer deps.Close()
		a.ServeOps(ctx, common.OpsAddr)

		c := kisa.NewCollector(url, apiKey, &http.Client{Timeout: timeout}, deps.Pool, deps.Cache)
		jobs.Loop(ctx, jobs.Job{Name: "kisa-collector", Every: every, Timeout: timeout, Run: c.Run},
			deps.Pool, deps.Redis, a.Metrics, a.Log)
		return nil
	})
}
