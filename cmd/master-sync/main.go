// Command master-sync copies the relay's customers and API keys into
// PostgreSQL and Redis.
package main

import (
	"context"
	"net/http"
	"time"

	"xrayplatform/internal/app"
	"xrayplatform/internal/boot"
	"xrayplatform/internal/config"
	"xrayplatform/internal/jobs"
	"xrayplatform/internal/mastersync"
)

func main() {
	app.Main("master-sync", func(ctx context.Context, a *app.App) error {
		var env config.Env
		common := config.LoadCommon(&env, ":9106")
		url := env.Required("RELAY_CUSTOMERS_URL")
		token := env.Str("RELAY_API_TOKEN", "")
		every := env.Dur("EVERY", 5*time.Minute)
		timeout := env.Dur("RUN_TIMEOUT", time.Minute)
		if err := env.Err(); err != nil {
			return err
		}
		deps, err := boot.Open(ctx, a, common)
		if err != nil {
			return err
		}
		defer deps.Close()
		a.ServeOps(ctx, common.OpsAddr)

		s := mastersync.New(url, token, &http.Client{Timeout: timeout}, deps.Pool, deps.Cache)
		jobs.Loop(ctx, jobs.Job{Name: "master-sync", Every: every, Timeout: timeout, Run: s.Run},
			deps.Pool, deps.Redis, a.Metrics, a.Log)
		return nil
	})
}
