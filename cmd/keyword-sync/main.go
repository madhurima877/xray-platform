// Command keyword-sync pushes the X-Ray keyword list to the relay after
// every change and once a day in full.
package main

import (
	"context"
	"net/http"
	"time"

	"xrayplatform/internal/app"
	"xrayplatform/internal/boot"
	"xrayplatform/internal/config"
	"xrayplatform/internal/jobs"
	"xrayplatform/internal/keywordsync"
)

func main() {
	app.Main("keyword-sync", func(ctx context.Context, a *app.App) error {
		var env config.Env
		common := config.LoadCommon(&env, ":9107")
		url := env.Required("RELAY_KEYWORDS_URL")
		token := env.Str("RELAY_API_TOKEN", "")
		every := env.Dur("EVERY", 30*time.Second)
		fullEvery := env.Dur("FULL_EVERY", 24*time.Hour)
		timeout := env.Dur("RUN_TIMEOUT", 30*time.Second)
		if err := env.Err(); err != nil {
			return err
		}
		deps, err := boot.Open(ctx, a, common)
		if err != nil {
			return err
		}
		defer deps.Close()
		a.ServeOps(ctx, common.OpsAddr)

		s := keywordsync.New(url, token, fullEvery, &http.Client{Timeout: timeout}, deps.Pool, deps.Cache)
		jobs.Loop(ctx, jobs.Job{Name: "keyword-sync", Every: every, Timeout: timeout, Run: s.Run},
			deps.Pool, deps.Redis, a.Metrics, a.Log)
		return nil
	})
}
