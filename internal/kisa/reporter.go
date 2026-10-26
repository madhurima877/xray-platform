// Package kisa holds both KISA services.
//
// kisa-reporter (this file): BLOCK verdicts go into the kisa_outbox table
// before the Kafka offset is committed; a sender loop posts them to KISA
// with retries and backoff (S-11). A row is leased while it is being sent,
// so several reporter copies can run side by side.
//
// kisa-collector (collector.go): pulls KISA's bad-URL list into the Black
// List.
package kisa

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"xrayplatform/internal/app"
	"xrayplatform/internal/model"
)

// Report is the body sent to KISA. The real field names come from KISA's
// API spec; adjust here once it is available.
type Report struct {
	ReportID   string         `json:"report_id"`
	MessageID  string         `json:"message_id"`
	CustomerID string         `json:"customer_id"`
	Sender     string         `json:"sender,omitempty"`
	URLs       []string       `json:"urls,omitempty"`
	Reasons    []model.Reason `json:"reasons"`
	DecidedAt  time.Time      `json:"decided_at"`
}

func NewReport(e model.Event) Report {
	return Report{ReportID: e.Key(), MessageID: e.MessageID, CustomerID: e.CustomerID, Sender: e.Sender,
		URLs: e.URLs, Reasons: e.Reasons, DecidedAt: e.DecidedAt}
}

// Enqueue is the Kafka handler: store every BLOCK in the outbox.
func Enqueue(pool *pgxpool.Pool) func(ctx context.Context, events []model.Event) error {
	return func(ctx context.Context, events []model.Event) error {
		b := &pgx.Batch{}
		for _, e := range events {
			if e.Action != model.Block {
				continue
			}
			payload, err := json.Marshal(NewReport(e))
			if err != nil {
				return err
			}
			b.Queue(`INSERT INTO kisa_outbox (customer_id, message_id, payload) VALUES ($1, $2, $3)
				ON CONFLICT DO NOTHING`, e.CustomerID, e.MessageID, payload)
		}
		if b.Len() == 0 {
			return nil
		}
		return pool.SendBatch(ctx, b).Close()
	}
}

type SenderConfig struct {
	URL         string
	APIKey      string
	Timeout     time.Duration
	Every       time.Duration
	BatchSize   int
	MaxAttempts int
	Lease       time.Duration // how long a claimed row is hidden from other senders
}

type Sender struct {
	cfg     SenderConfig
	pool    *pgxpool.Pool
	client  *http.Client
	metrics *app.Metrics
	log     *slog.Logger
}

func NewSender(cfg SenderConfig, pool *pgxpool.Pool, m *app.Metrics, log *slog.Logger) *Sender {
	return &Sender{cfg: cfg, pool: pool, client: &http.Client{Timeout: cfg.Timeout}, metrics: m, log: log}
}

// Run sends due reports until ctx is done.
func (s *Sender) Run(ctx context.Context) {
	t := time.NewTicker(s.cfg.Every)
	defer t.Stop()
	for {
		for {
			n, err := s.SendDue(ctx)
			if err != nil {
				s.log.Warn("send round failed", "err", err)
			}
			if err != nil || n < s.cfg.BatchSize || ctx.Err() != nil {
				break // keep going while there is a backlog
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

type row struct {
	customerID, messageID string
	payload               []byte
	attempts              int
}

// SendDue claims up to BatchSize due rows and sends them one by one.
func (s *Sender) SendDue(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE kisa_outbox o SET next_attempt_at = now() + $2::interval
		FROM (SELECT customer_id, message_id FROM kisa_outbox
		      WHERE status = 'pending' AND next_attempt_at <= now()
		      ORDER BY next_attempt_at LIMIT $1 FOR UPDATE SKIP LOCKED) d
		WHERE o.customer_id = d.customer_id AND o.message_id = d.message_id
		RETURNING o.customer_id, o.message_id, o.payload, o.attempts`,
		s.cfg.BatchSize, fmt.Sprintf("%d milliseconds", s.cfg.Lease.Milliseconds()))
	if err != nil {
		return 0, err
	}
	claimed, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var x row
		err := r.Scan(&x.customerID, &x.messageID, &x.payload, &x.attempts)
		return x, err
	})
	if err != nil {
		return 0, err
	}
	for _, r := range claimed {
		// Finish the claimed rows even during shutdown; each send has its own timeout.
		s.deliver(context.WithoutCancel(ctx), r)
	}
	return len(claimed), nil
}

func (s *Sender) deliver(ctx context.Context, r row) {
	status, err := s.post(ctx, r.payload)
	attempts := r.attempts + 1
	var q string
	var args []any
	switch {
	case err == nil:
		s.metrics.Inc("reports_sent")
		q = `UPDATE kisa_outbox SET status = 'sent', attempts = $3, sent_at = now(), last_error = ''
			WHERE customer_id = $1 AND message_id = $2`
		args = []any{r.customerID, r.messageID, attempts}
	case permanent(status) || attempts >= s.cfg.MaxAttempts:
		s.metrics.Inc("reports_failed")
		s.log.Warn("report failed for good", "customer_id", r.customerID, "message_id", r.messageID,
			"attempts", attempts, "err", err)
		q = `UPDATE kisa_outbox SET status = 'failed', attempts = $3, last_error = $4
			WHERE customer_id = $1 AND message_id = $2`
		args = []any{r.customerID, r.messageID, attempts, err.Error()}
	default:
		s.metrics.Inc("reports_retried")
		q = `UPDATE kisa_outbox SET attempts = $3, last_error = $4, next_attempt_at = now() + $5::interval
			WHERE customer_id = $1 AND message_id = $2`
		args = []any{r.customerID, r.messageID, attempts, err.Error(),
			fmt.Sprintf("%d milliseconds", Backoff(attempts).Milliseconds())}
	}
	if _, uerr := s.pool.Exec(ctx, q, args...); uerr != nil {
		// The lease runs out and the row is retried; KISA must accept duplicates by report_id.
		s.log.Warn("outbox update failed", "message_id", r.messageID, "err", uerr)
	}
}

// post returns the HTTP status (0 on a network error) and an error for any
// non-2xx answer.
func (s *Sender) post(ctx context.Context, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.APIKey)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode/100 == 2 {
		return resp.StatusCode, nil
	}
	return resp.StatusCode, fmt.Errorf("kisa answered %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
}

// permanent is a 4xx other than 408/429: retrying the same body won't help.
func permanent(status int) bool {
	return status >= 400 && status < 500 && status != http.StatusRequestTimeout && status != http.StatusTooManyRequests
}

// Backoff is 5 s × 2^(attempt-1) with ±20 % jitter, capped at 1 h.
func Backoff(attempt int) time.Duration {
	d := 5 * time.Second
	for i := 1; i < attempt && d < time.Hour; i++ {
		d *= 2
	}
	d = min(d, time.Hour)
	jitter := time.Duration(rand.Int64N(int64(d)/5*2+1)) - d/5
	return d + jitter
}
