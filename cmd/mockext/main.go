// Command mockext stands in for the outside systems during local runs:
//
//	GET  /relay/customers   relay customer master (for master-sync)
//	PUT  /relay/keywords    relay keyword push (for keyword-sync)
//	GET  /kisa/badurls      KISA bad-URL list (for kisa-collector)
//	POST /kisa/report       KISA report intake (for kisa-reporter)
//	GET  /seen              what was pushed/reported so far
//
// The relay callback for xray-api is x-ray's own cmd/mockrelay.
package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

type state struct {
	mu       sync.Mutex
	keywords json.RawMessage
	reports  []json.RawMessage
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "mockext")
	addr := os.Getenv("MOCKEXT_ADDR")
	if addr == "" {
		addr = ":9000"
	}
	callback := os.Getenv("DEMO_CALLBACK_URL")
	if callback == "" {
		callback = "http://mockrelay:9000/callback"
	}
	st := &state{}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /relay/customers", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"customers": []map[string]any{
			{"id": "cust-demo", "name": "Demo Bank", "callback_url": callback, "active": true, "api_keys": []string{"demo-key"}},
			{"id": "cust-shop", "name": "Demo Shop", "callback_url": callback, "active": true, "api_keys": []string{"shop-key"}},
		}})
	})
	mux.HandleFunc("PUT /relay/keywords", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		st.mu.Lock()
		st.keywords = b
		st.mu.Unlock()
		log.Info("relay got keywords", "body", string(b))
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /kisa/badurls", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "# KISA smishing URL list (mock)\nhttp://evil.example/login\nphish.example\nhttps://www.fake-bank.example/kyc\n")
	})
	mux.HandleFunc("POST /kisa/report", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		st.mu.Lock()
		st.reports = append(st.reports, b)
		st.mu.Unlock()
		log.Info("kisa got report", "body", string(b))
		writeJSON(w, map[string]any{"received_at": time.Now().UTC()})
	})
	mux.HandleFunc("GET /seen", func(w http.ResponseWriter, _ *http.Request) {
		st.mu.Lock()
		defer st.mu.Unlock()
		writeJSON(w, map[string]any{"keywords": st.keywords, "reports": st.reports})
	})

	log.Info("listening", "addr", addr)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		log.Error("stopped", "err", err)
		os.Exit(1)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
