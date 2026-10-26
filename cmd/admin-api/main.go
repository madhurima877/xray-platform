// Command admin-api is the REST backend of the admin web: login and roles,
// customers and levels, Black/White List, keywords, statistics, history,
// KISA reports, sync jobs and the audit log.
package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"xrayplatform/internal/admin"
	"xrayplatform/internal/app"
	"xrayplatform/internal/boot"
	"xrayplatform/internal/config"
)

func main() {
	app.Main("admin-api", func(ctx context.Context, a *app.App) error {
		var env config.Env
		common := config.LoadCommon(&env, "")
		addr := env.Str("ADMIN_ADDR", ":8090")
		secret := env.Required("JWT_SECRET")
		cfg := admin.Config{
			Secret:     []byte(secret),
			TokenTTL:   env.Dur("TOKEN_TTL", 8*time.Hour),
			CORSOrigin: env.Str("CORS_ORIGIN", ""),
		}
		bootUser := env.Str("ADMIN_BOOTSTRAP_USER", "")
		bootPass := env.Str("ADMIN_BOOTSTRAP_PASSWORD", "")
		if err := env.Err(); err != nil {
			return err
		}
		if len(secret) < 32 {
			return errors.New("JWT_SECRET must be at least 32 characters")
		}
		deps, err := boot.Open(ctx, a, common)
		if err != nil {
			return err
		}
		defer deps.Close()

		s := admin.New(cfg, deps.Pool, deps.Redis, deps.Cache, a)
		if err := s.Bootstrap(ctx, bootUser, bootPass); err != nil {
			return err
		}
		srv := &http.Server{
			Addr:              addr,
			Handler:           s.Routes(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       90 * time.Second,
		}
		serveErr := make(chan error, 1)
		go func() {
			a.Log.Info("listening", "addr", addr)
			serveErr <- srv.ListenAndServe()
		}()
		select {
		case err := <-serveErr:
			return err
		case <-ctx.Done():
		}
		a.SetDraining()
		sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(sctx)
	})
}
