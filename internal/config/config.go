// Package config reads settings from environment variables. Every binary
// builds its own Config from an Env, so a bad value is reported once, with
// all other bad values, before anything starts.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Env struct{ errs []error }

func (e *Env) Str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// Required records an error when key is unset.
func (e *Env) Required(key string) string {
	v := os.Getenv(key)
	if v == "" {
		e.errs = append(e.errs, fmt.Errorf("%s is required", key))
	}
	return v
}

func (e *Env) Int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

func (e *Env) Dur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a positive duration", key, v))
		return def
	}
	return d
}

func (e *Env) Bool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a boolean", key, v))
		return def
	}
	return b
}

// List splits a comma-separated value and drops empty items.
func (e *Env) List(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func (e *Env) Err() error { return errors.Join(e.errs...) }

// Common is what every service needs.
type Common struct {
	OpsAddr       string // /healthz, /readyz, /metrics
	DatabaseURL   string
	DBMaxConns    int
	RedisAddr     string
	RedisPassword string
	RedisDB       int
}

func LoadCommon(e *Env, defaultOps string) Common {
	return Common{
		OpsAddr:       e.Str("OPS_ADDR", defaultOps),
		DatabaseURL:   e.Required("DATABASE_URL"),
		DBMaxConns:    e.Int("DB_MAX_CONNS", 10),
		RedisAddr:     e.Str("REDIS_ADDR", "localhost:6379"),
		RedisPassword: e.Str("REDIS_PASSWORD", ""),
		RedisDB:       e.Int("REDIS_DB", 0),
	}
}

// Kafka is the consumer side of the xray-api result events.
type Kafka struct {
	Brokers    []string
	Topic      string
	Group      string
	BatchSize  int
	FlushEvery time.Duration
}

func LoadKafka(e *Env, defaultGroup string) Kafka {
	k := Kafka{
		Brokers:    e.List("KAFKA_BROKERS", nil),
		Topic:      e.Str("KAFKA_TOPIC", "xray.verdicts"),
		Group:      e.Str("KAFKA_GROUP", defaultGroup),
		BatchSize:  e.Int("BATCH_SIZE", 500),
		FlushEvery: e.Dur("FLUSH_EVERY", time.Second),
	}
	if len(k.Brokers) == 0 {
		e.errs = append(e.errs, errors.New("KAFKA_BROKERS is required"))
	}
	if k.BatchSize < 1 {
		e.errs = append(e.errs, errors.New("BATCH_SIZE must be at least 1"))
	}
	return k
}
