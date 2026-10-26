// Package boot opens what most binaries need: PostgreSQL (with migrations),
// Redis and the Redis cache builder, and adds them to /readyz.
package boot

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"xrayplatform/internal/app"
	"xrayplatform/internal/cache"
	"xrayplatform/internal/config"
	"xrayplatform/internal/db"
)

type Deps struct {
	Pool  *pgxpool.Pool
	Redis *redis.Client
	Cache *cache.Cache
}

func Open(ctx context.Context, a *app.App, c config.Common) (*Deps, error) {
	pool, err := db.Open(ctx, c.DatabaseURL, c.DBMaxConns)
	if err != nil {
		return nil, err
	}
	if err := db.Migrate(ctx, pool, a.Log); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: c.RedisAddr, Password: c.RedisPassword, DB: c.RedisDB})
	a.Ready("postgres", pool.Ping)
	a.Ready("redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() })
	return &Deps{Pool: pool, Redis: rdb, Cache: cache.New(rdb, pool)}, nil
}

func (d *Deps) Close() {
	d.Redis.Close()
	d.Pool.Close()
}
