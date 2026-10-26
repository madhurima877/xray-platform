package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"xrayplatform/internal/jobs"
)

const (
	statsMaxSpan   = 400 * 24 * time.Hour
	historyMaxSpan = 31 * 24 * time.Hour
)

// statsFilter applies customer_id and the time range to stats_minute.
func statsFilter(r *http.Request) (filter, error) {
	from, to, err := timeRange(r, statsMaxSpan)
	if err != nil {
		return filter{}, err
	}
	var f filter
	f.add("bucket >= ?", from)
	f.add("bucket < ?", to)
	if v := r.URL.Query().Get("customer_id"); v != "" {
		f.add("customer_id = ?", v)
	}
	return f, nil
}

// statsSummary is the S-01 dashboard header: counts by verdict and latency.
func (s *Server) statsSummary(w http.ResponseWriter, r *http.Request) error {
	f, err := statsFilter(r)
	if err != nil {
		return err
	}
	type row struct {
		Verdict    string  `db:"verdict"`
		Count      int64   `db:"count"`
		LatencySum float64 `db:"latency_sum"`
		LatencyMax float64 `db:"latency_max"`
	}
	rows, err := collect[row](r.Context(), s.pool, `SELECT verdict, sum(count)::bigint AS count,
		sum(latency_sum_ms) AS latency_sum, max(latency_max_ms) AS latency_max
		FROM stats_minute`+f.where()+` GROUP BY verdict`, f.args...)
	if err != nil {
		return err
	}
	by := map[string]int64{"ALLOW": 0, "HOLD": 0, "BLOCK": 0, "FAIL_OPEN": 0}
	var total int64
	var sum, maxLat float64
	for _, x := range rows {
		by[x.Verdict] = x.Count
		total += x.Count
		sum += x.LatencySum
		maxLat = max(maxLat, x.LatencyMax)
	}
	avg := 0.0
	if total > 0 {
		avg = sum / float64(total)
	}
	return writeJSON(w, http.StatusOK, map[string]any{"total": total, "by_verdict": by,
		"avg_latency_ms": avg, "max_latency_ms": maxLat})
}

// statsSeries is the S-01/S-05 chart: counts per interval and verdict.
func (s *Server) statsSeries(w http.ResponseWriter, r *http.Request) error {
	interval := r.URL.Query().Get("interval")
	if interval == "" {
		interval = "hour"
	}
	if !slices.Contains([]string{"minute", "hour", "day"}, interval) {
		return badRequest("interval must be minute, hour or day")
	}
	f, err := statsFilter(r)
	if err != nil {
		return err
	}
	type point struct {
		T       time.Time `json:"t" db:"t"`
		Verdict string    `json:"verdict" db:"verdict"`
		Count   int64     `json:"count" db:"count"`
	}
	trunc := f.arg(interval)
	points, err := collect[point](r.Context(), s.pool, `SELECT date_trunc(`+trunc+`, bucket, 'UTC') AS t, verdict,
		sum(count)::bigint AS count FROM stats_minute`+f.where()+` GROUP BY 1, 2 ORDER BY 1, 2 LIMIT 20000`, f.args...)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]any{"interval": interval, "points": points})
}

// statsReasons is S-05 detection: which step decided how often.
func (s *Server) statsReasons(w http.ResponseWriter, r *http.Request) error {
	from, to, err := timeRange(r, statsMaxSpan)
	if err != nil {
		return err
	}
	var f filter
	f.add("day >= ?::date", from)
	f.add("day <= ?::date", to)
	if v := r.URL.Query().Get("customer_id"); v != "" {
		f.add("customer_id = ?", v)
	}
	type row struct {
		Step   string `json:"step" db:"step"`
		Action string `json:"action" db:"action"`
		Count  int64  `json:"count" db:"count"`
	}
	rows, err := collect[row](r.Context(), s.pool, `SELECT step, action, sum(count)::bigint AS count
		FROM stats_reason_daily`+f.where()+` GROUP BY 1, 2 ORDER BY 3 DESC`, f.args...)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]any{"items": rows})
}

type historyRow struct {
	CustomerID   string          `json:"customer_id" db:"customer_id"`
	MessageID    string          `json:"message_id" db:"message_id"`
	DecidedAt    time.Time       `json:"decided_at" db:"decided_at"`
	Verdict      string          `json:"verdict" db:"verdict"`
	Level        int16           `json:"level" db:"level"`
	Sender       string          `json:"sender" db:"sender"`
	URLs         []string        `json:"urls" db:"urls"`
	Reasons      json.RawMessage `json:"reasons" db:"reasons"`
	AIScore      *float32        `json:"ai_score" db:"ai_score"`
	ModelVersion string          `json:"model_version" db:"model_version"`
	LatencyMS    float32         `json:"latency_ms" db:"latency_ms"`
}

// history is S-12. It pages backwards by decided_at with ?before=, and the
// time range keeps the scan to a few daily partitions.
func (s *Server) history(w http.ResponseWriter, r *http.Request) error {
	from, to, err := timeRange(r, historyMaxSpan)
	if err != nil {
		return err
	}
	limit, _, err := pageParams(r, 50, 500)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	var f filter
	f.add("decided_at >= ?", from)
	f.add("decided_at < ?", to)
	if v := q.Get("before"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return badRequest("before must be RFC 3339")
		}
		f.add("decided_at < ?", t)
	}
	for _, p := range []struct{ param, col string }{
		{"customer_id", "customer_id"}, {"message_id", "message_id"}, {"verdict", "verdict"}, {"sender", "sender"},
	} {
		if v := q.Get(p.param); v != "" {
			f.add(p.col+" = ?", v)
		}
	}
	where := f.where()
	items, err := collect[historyRow](r.Context(), s.pool, `SELECT customer_id, message_id, decided_at, verdict, level,
		sender, urls, reasons, ai_score, model_version, latency_ms FROM verdict_history`+where+
		` ORDER BY decided_at DESC LIMIT `+f.arg(limit), f.args...)
	if err != nil {
		return err
	}
	resp := map[string]any{"items": items}
	if len(items) == limit {
		resp["next_before"] = items[len(items)-1].DecidedAt.Format(time.RFC3339Nano)
	}
	return writeJSON(w, http.StatusOK, resp)
}

type reportRow struct {
	CustomerID    string          `json:"customer_id" db:"customer_id"`
	MessageID     string          `json:"message_id" db:"message_id"`
	Payload       json.RawMessage `json:"payload" db:"payload"`
	Status        string          `json:"status" db:"status"`
	Attempts      int             `json:"attempts" db:"attempts"`
	NextAttemptAt time.Time       `json:"next_attempt_at" db:"next_attempt_at"`
	LastError     string          `json:"last_error" db:"last_error"`
	CreatedAt     time.Time       `json:"created_at" db:"created_at"`
	SentAt        *time.Time      `json:"sent_at" db:"sent_at"`
}

const reportColumns = `customer_id, message_id, payload, status, attempts, next_attempt_at, last_error, created_at, sent_at`

// listReports is S-11: the outbox and counts per status.
func (s *Server) listReports(w http.ResponseWriter, r *http.Request) error {
	limit, offset, err := pageParams(r, 50, 500)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	var f filter
	if v := q.Get("status"); v != "" {
		f.add("status = ?", v)
	}
	if v := q.Get("customer_id"); v != "" {
		f.add("customer_id = ?", v)
	}
	ctx := r.Context()
	where := f.where()
	items, err := collect[reportRow](ctx, s.pool, `SELECT `+reportColumns+` FROM kisa_outbox`+where+
		` ORDER BY created_at DESC LIMIT `+f.arg(limit)+` OFFSET `+f.arg(offset), f.args...)
	if err != nil {
		return err
	}
	type count struct {
		Status string `db:"status"`
		N      int64  `db:"n"`
	}
	counts, err := collect[count](ctx, s.pool, `SELECT status, count(*) AS n FROM kisa_outbox GROUP BY status`)
	if err != nil {
		return err
	}
	by := map[string]int64{"pending": 0, "sent": 0, "failed": 0}
	for _, c := range counts {
		by[c.Status] = c.N
	}
	return writeJSON(w, http.StatusOK, map[string]any{"items": items, "counts": by})
}

// retryReport puts a failed (or waiting) report back at the front of the queue.
func (s *Server) retryReport(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	cust, msg := r.PathValue("customer"), r.PathValue("message")
	var out reportRow
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := collect[reportRow](ctx, tx, `UPDATE kisa_outbox SET status = 'pending', attempts = 0,
			next_attempt_at = now(), last_error = '' WHERE customer_id = $1 AND message_id = $2 AND status <> 'sent'
			RETURNING `+reportColumns, cust, msg)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return notFound("unsent report")
		}
		out = rows[0]
		return s.audit(ctx, tx, userFrom(ctx), "retry", "kisa_report", cust+"/"+msg, nil)
	})
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, out)
}

type runRow struct {
	ID         int64     `json:"id" db:"id"`
	Job        string    `json:"job" db:"job"`
	StartedAt  time.Time `json:"started_at" db:"started_at"`
	FinishedAt time.Time `json:"finished_at" db:"finished_at"`
	OK         bool      `json:"ok" db:"ok"`
	Items      int       `json:"items" db:"items"`
	Error      string    `json:"error" db:"error"`
}

const runColumns = `id, job, started_at, finished_at, ok, items, error`

// syncStatus is the S-14 header: the last run and last success of each job.
func (s *Server) syncStatus(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	type status struct {
		Job       string  `json:"job"`
		LastRun   *runRow `json:"last_run"`
		LastOKRun *runRow `json:"last_ok_run"`
	}
	out := make([]status, 0, len(jobs.Names))
	for _, name := range jobs.Names {
		st := status{Job: name}
		last, err := collect[runRow](ctx, s.pool, `SELECT `+runColumns+` FROM sync_runs WHERE job = $1
			ORDER BY started_at DESC LIMIT 1`, name)
		if err != nil {
			return err
		}
		lastOK, err := collect[runRow](ctx, s.pool, `SELECT `+runColumns+` FROM sync_runs WHERE job = $1 AND ok
			ORDER BY started_at DESC LIMIT 1`, name)
		if err != nil {
			return err
		}
		if len(last) > 0 {
			st.LastRun = &last[0]
		}
		if len(lastOK) > 0 {
			st.LastOKRun = &lastOK[0]
		}
		out = append(out, st)
	}
	return writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (s *Server) syncRuns(w http.ResponseWriter, r *http.Request) error {
	limit, offset, err := pageParams(r, 50, 500)
	if err != nil {
		return err
	}
	var f filter
	if v := r.URL.Query().Get("job"); v != "" {
		f.add("job = ?", v)
	}
	where := f.where()
	items, err := collect[runRow](r.Context(), s.pool, `SELECT `+runColumns+` FROM sync_runs`+where+
		` ORDER BY started_at DESC LIMIT `+f.arg(limit)+` OFFSET `+f.arg(offset), f.args...)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

// triggerSync is the "sync now" button: the job's service hears it over
// Redis pub/sub and runs at once.
func (s *Server) triggerSync(w http.ResponseWriter, r *http.Request) error {
	job := r.PathValue("job")
	if !slices.Contains(jobs.Names, job) {
		return notFound("job")
	}
	ctx := r.Context()
	if err := jobs.Trigger(ctx, s.rdb, job); err != nil {
		return fmt.Errorf("trigger %s: %w", job, err)
	}
	s.audit(ctx, s.pool, userFrom(ctx), "trigger", "sync_job", job, nil)
	return writeJSON(w, http.StatusAccepted, map[string]string{"job": job, "status": "triggered"})
}

type auditRow struct {
	ID       int64           `json:"id" db:"id"`
	At       time.Time       `json:"at" db:"at"`
	UserID   *int64          `json:"user_id" db:"user_id"`
	Username string          `json:"username" db:"username"`
	Action   string          `json:"action" db:"action"`
	Entity   string          `json:"entity" db:"entity"`
	EntityID string          `json:"entity_id" db:"entity_id"`
	Detail   json.RawMessage `json:"detail" db:"detail"`
}

func (s *Server) auditLog(w http.ResponseWriter, r *http.Request) error {
	limit, offset, err := pageParams(r, 100, 1000)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	var f filter
	if v := q.Get("username"); v != "" {
		f.add("username = ?", v)
	}
	if v := q.Get("entity"); v != "" {
		f.add("entity = ?", v)
	}
	where := f.where()
	items, err := collect[auditRow](r.Context(), s.pool, `SELECT id, at, user_id, username, action, entity, entity_id, detail
		FROM audit_log`+where+` ORDER BY at DESC, id DESC LIMIT `+f.arg(limit)+` OFFSET `+f.arg(offset), f.args...)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]any{"items": items})
}
