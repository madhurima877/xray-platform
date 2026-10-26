package admin

import (
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

type customerRow struct {
	ID            string     `json:"id" db:"id"`
	Name          string     `json:"name" db:"name"`
	CallbackURL   string     `json:"callback_url" db:"callback_url"`
	Active        bool       `json:"active" db:"active"`
	Level         int        `json:"level" db:"level"`
	RateLimit     int        `json:"rate_limit" db:"rate_limit"`
	KeywordAction string     `json:"keyword_action" db:"keyword_action"`
	AIBlock       float32    `json:"ai_block" db:"ai_block"`
	AIHold        float32    `json:"ai_hold" db:"ai_hold"`
	APIKeys       int        `json:"api_keys" db:"api_keys"`
	SyncedAt      *time.Time `json:"synced_at" db:"synced_at"`
	UpdatedAt     time.Time  `json:"updated_at" db:"updated_at"`
}

const customerSelect = `SELECT c.id, c.name, c.callback_url, c.active, c.level, c.rate_limit, c.keyword_action,
	c.ai_block, c.ai_hold, (SELECT count(*) FROM api_keys k WHERE k.customer_id = c.id)::int AS api_keys,
	c.synced_at, c.updated_at FROM customers c`

func (s *Server) listCustomers(w http.ResponseWriter, r *http.Request) error {
	limit, offset, err := pageParams(r, 100, 1000)
	if err != nil {
		return err
	}
	var f filter
	if q := r.URL.Query().Get("q"); q != "" {
		f.add("(c.id ILIKE ? OR c.name ILIKE ?)", "%"+q+"%") // both ? are the same argument
	}
	if v := r.URL.Query().Get("active"); v != "" {
		f.add("c.active = ?", v == "true")
	}
	ctx := r.Context()
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM customers c`+f.where(), f.args...).Scan(&total); err != nil {
		return err
	}
	where := f.where()
	items, err := collect[customerRow](ctx, s.pool,
		customerSelect+where+` ORDER BY c.id LIMIT `+f.arg(limit)+` OFFSET `+f.arg(offset), f.args...)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (s *Server) getCustomer(w http.ResponseWriter, r *http.Request) error {
	items, err := collect[customerRow](r.Context(), s.pool, customerSelect+` WHERE c.id = $1`, r.PathValue("id"))
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return notFound("customer")
	}
	return writeJSON(w, http.StatusOK, items[0])
}

// updateCustomerRequest holds the fields X-Ray owns. Name, callback and API
// keys come from the relay through master-sync.
type updateCustomerRequest struct {
	Level         *int     `json:"level"`
	RateLimit     *int     `json:"rate_limit"`
	KeywordAction *string  `json:"keyword_action"`
	AIBlock       *float32 `json:"ai_block"`
	AIHold        *float32 `json:"ai_hold"`
}

func (req updateCustomerRequest) apply(c *customerRow) error {
	if req.Level != nil {
		c.Level = *req.Level
	}
	if req.RateLimit != nil {
		c.RateLimit = *req.RateLimit
	}
	if req.KeywordAction != nil {
		c.KeywordAction = *req.KeywordAction
	}
	if req.AIBlock != nil {
		c.AIBlock = *req.AIBlock
	}
	if req.AIHold != nil {
		c.AIHold = *req.AIHold
	}
	switch {
	case c.Level < 1 || c.Level > 5:
		return badRequest("level must be 1–5 (T1–T5)")
	case c.RateLimit < 0:
		return badRequest("rate_limit must be ≥ 0 (0 = no limit)")
	case c.KeywordAction != "HOLD" && c.KeywordAction != "BLOCK":
		return badRequest("keyword_action must be HOLD or BLOCK")
	case c.AIHold < 0 || c.AIBlock > 1 || c.AIHold > c.AIBlock:
		return badRequest("need 0 ≤ ai_hold ≤ ai_block ≤ 1")
	}
	return nil
}

func (s *Server) updateCustomer(w http.ResponseWriter, r *http.Request) error {
	var req updateCustomerRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	ctx := r.Context()
	id := r.PathValue("id")
	var before, after customerRow
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		items, err := collect[customerRow](ctx, tx, customerSelect+` WHERE c.id = $1 FOR UPDATE OF c`, id)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return notFound("customer")
		}
		before, after = items[0], items[0]
		if err := req.apply(&after); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE customers SET level = $2, rate_limit = $3, keyword_action = $4,
			ai_block = $5, ai_hold = $6, updated_at = now() WHERE id = $1`,
			id, after.Level, after.RateLimit, after.KeywordAction, after.AIBlock, after.AIHold); err != nil {
			return err
		}
		return s.audit(ctx, tx, userFrom(ctx), "update", "customer", id, map[string]any{
			"before": policy(before), "after": policy(after)})
	})
	if err != nil {
		return err
	}
	after.UpdatedAt = time.Now().UTC()
	ok := s.refreshed("customer "+id, s.cache.SyncCustomer(ctx, id))
	return writeJSON(w, http.StatusOK, map[string]any{"customer": after, "redis_refreshed": ok})
}

func policy(c customerRow) map[string]any {
	return map[string]any{"level": c.Level, "rate_limit": c.RateLimit, "keyword_action": c.KeywordAction,
		"ai_block": c.AIBlock, "ai_hold": c.AIHold}
}
