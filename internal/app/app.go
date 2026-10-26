// Package app holds what every binary shares: JSON logging, the
// SIGINT/SIGTERM context, the ops endpoints (/healthz, /readyz, /metrics)
// and simple counters.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type App struct {
	Name    string
	Log     *slog.Logger
	Metrics *Metrics

	mu       sync.Mutex
	checks   map[string]func(context.Context) error
	draining atomic.Bool
}

// Main runs fn with a context cancelled on SIGINT/SIGTERM and exits 1 when
// fn fails.
func Main(name string, fn func(ctx context.Context, a *App) error) {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", name)
	a := &App{Name: name, Log: log, Metrics: NewMetrics(name), checks: map[string]func(context.Context) error{}}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("starting")
	if err := fn(ctx, a); err != nil {
		log.Error("stopped", "err", err)
		os.Exit(1)
	}
	log.Info("stopped")
}

// Ready adds a dependency check to /readyz.
func (a *App) Ready(name string, check func(context.Context) error) {
	a.mu.Lock()
	a.checks[name] = check
	a.mu.Unlock()
}

// SetDraining makes /readyz fail from now on.
func (a *App) SetDraining() { a.draining.Store(true) }

// Register adds the ops endpoints to mux.
func (a *App) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", a.readyz)
	mux.Handle("GET /metrics", a.Metrics)
}

func (a *App) readyz(w http.ResponseWriter, r *http.Request) {
	if a.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	a.mu.Lock()
	checks := make(map[string]func(context.Context) error, len(a.checks))
	for k, v := range a.checks {
		checks[k] = v
	}
	a.mu.Unlock()
	for name, check := range checks {
		if err := check(ctx); err != nil {
			http.Error(w, name+": "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	w.Write([]byte("ready"))
}

// ServeOps runs the ops endpoints on their own port until ctx is done.
// Workers and jobs use this; admin-api puts them on its main mux instead.
func (a *App) ServeOps(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	a.Register(mux)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.Log.Error("ops server failed", "addr", addr, "err", err)
		}
	}()
}

// Metrics is a set of named counters served as JSON.
type Metrics struct {
	service string
	started time.Time
	mu      sync.RWMutex
	m       map[string]*atomic.Int64
}

func NewMetrics(service string) *Metrics {
	return &Metrics{service: service, started: time.Now(), m: map[string]*atomic.Int64{}}
}

func (m *Metrics) counter(name string) *atomic.Int64 {
	m.mu.RLock()
	c, ok := m.m[name]
	m.mu.RUnlock()
	if ok {
		return c
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok = m.m[name]; !ok {
		c = new(atomic.Int64)
		m.m[name] = c
	}
	return c
}

func (m *Metrics) Add(name string, n int64) { m.counter(name).Add(n) }
func (m *Metrics) Inc(name string)          { m.counter(name).Add(1) }
func (m *Metrics) Set(name string, n int64) { m.counter(name).Store(n) }
func (m *Metrics) Get(name string) int64    { return m.counter(name).Load() }

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	m.mu.RLock()
	names := make([]string, 0, len(m.m))
	for k := range m.m {
		names = append(names, k)
	}
	sort.Strings(names)
	counters := make(map[string]int64, len(names))
	for _, k := range names {
		counters[k] = m.m[k].Load()
	}
	m.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"service":    m.service,
		"uptime_sec": int64(time.Since(m.started).Seconds()),
		"counters":   counters,
	})
}
