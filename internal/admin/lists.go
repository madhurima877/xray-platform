package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"xrayplatform/internal/cache"
	"xrayplatform/internal/hosts"
)

type entryRow struct {
	ID         int64     `json:"id" db:"id"`
	List       string    `json:"list" db:"list"`
	Kind       string    `json:"kind" db:"kind"`
	Value      string    `json:"value" db:"value"`
	CustomerID string    `json:"customer_id" db:"customer_id"`
	Source     string    `json:"source" db:"source"`
	Note       string    `json:"note" db:"note"`
	CreatedBy  string    `json:"created_by" db:"created_by"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
}

const entryColumns = `id, list, kind, value, customer_id, source, note, created_by, created_at`

func listParam(r *http.Request) (string, error) {
	switch l := r.PathValue("list"); l {
	case "black", "white":
		return l, nil
	}
	return "", notFound("list")
}

// normalizeValue gives the exact string xray-api looks up.
func normalizeValue(kind, value string) (string, error) {
	switch kind {
	case "url_host":
		if h := hosts.Normalize(value); h != "" {
			return h, nil
		}
		return "", badRequest("value is not a URL or host name")
	case "sender":
		if v := strings.TrimSpace(value); v != "" && len(v) <= 64 {
			return v, nil
		}
		return "", badRequest("sender must be 1–64 characters")
	}
	return "", badRequest("kind must be url_host or sender")
}

func (s *Server) listEntries(w http.ResponseWriter, r *http.Request) error {
	list, err := listParam(r)
	if err != nil {
		return err
	}
	limit, offset, err := pageParams(r, 100, 1000)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	var f filter
	f.add("list = ?", list)
	if v, ok := q["customer_id"]; ok {
		f.add("customer_id = ?", v[0]) // "" selects the global list
	}
	if v := q.Get("kind"); v != "" {
		f.add("kind = ?", v)
	}
	if v := q.Get("source"); v != "" {
		f.add("source = ?", v)
	}
	if v := q.Get("q"); v != "" {
		f.add("value ILIKE ?", "%"+v+"%")
	}
	ctx := r.Context()
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM list_entries`+f.where(), f.args...).Scan(&total); err != nil {
		return err
	}
	where := f.where()
	items, err := collect[entryRow](ctx, s.pool, `SELECT `+entryColumns+` FROM list_entries`+where+
		` ORDER BY created_at DESC, id DESC LIMIT `+f.arg(limit)+` OFFSET `+f.arg(offset), f.args...)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

type addEntryRequest struct {
	Kind       string `json:"kind"`
	Value      string `json:"value"`
	CustomerID string `json:"customer_id"`
	Note       string `json:"note"`
}

func (s *Server) addEntry(w http.ResponseWriter, r *http.Request) error {
	list, err := listParam(r)
	if err != nil {
		return err
	}
	var req addEntryRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	value, err := normalizeValue(req.Kind, req.Value)
	if err != nil {
		return err
	}
	if len(req.Note) > 500 {
		return badRequest("note is longer than 500 characters")
	}
	ctx := r.Context()
	me := userFrom(ctx)
	var out entryRow
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := checkCustomer(ctx, tx, req.CustomerID); err != nil {
			return err
		}
		rows, err := collect[entryRow](ctx, tx, `INSERT INTO list_entries (list, kind, value, customer_id, source, note, created_by)
			VALUES ($1, $2, $3, $4, 'admin', $5, $6) RETURNING `+entryColumns,
			list, req.Kind, value, req.CustomerID, req.Note, me.Username)
		if err != nil {
			return err
		}
		out = rows[0]
		return s.audit(ctx, tx, me, "add", list+"_list", fmt.Sprint(out.ID),
			map[string]any{"kind": out.Kind, "value": out.Value, "customer_id": out.CustomerID})
	})
	if err != nil {
		return err
	}
	ok := s.refreshed(list+" list", s.cache.SetMember(ctx, cache.ListKey(list, out.CustomerID), out.Value, true))
	return writeJSON(w, http.StatusCreated, map[string]any{"entry": out, "redis_refreshed": ok})
}

func (s *Server) deleteEntry(w http.ResponseWriter, r *http.Request) error {
	list, err := listParam(r)
	if err != nil {
		return err
	}
	id, err := pathID(r, "id")
	if err != nil {
		return err
	}
	ctx := r.Context()
	var out entryRow
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := collect[entryRow](ctx, tx, `DELETE FROM list_entries WHERE id = $1 AND list = $2 RETURNING `+entryColumns,
			id, list)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return notFound("entry")
		}
		out = rows[0]
		return s.audit(ctx, tx, userFrom(ctx), "delete", list+"_list", fmt.Sprint(id),
			map[string]any{"kind": out.Kind, "value": out.Value, "customer_id": out.CustomerID, "source": out.Source})
	})
	if err != nil {
		return err
	}
	ok := s.refreshed(list+" list", s.cache.SetMember(ctx, cache.ListKey(list, out.CustomerID), out.Value, false))
	resp := map[string]any{"deleted": out, "redis_refreshed": ok}
	if out.Source == "kisa" {
		resp["warning"] = "this entry comes from the KISA feed and returns on the next kisa-collector run while KISA still lists it"
	}
	return writeJSON(w, http.StatusOK, resp)
}

func checkCustomer(ctx context.Context, tx pgx.Tx, id string) error {
	if id == "" {
		return nil
	}
	var one int
	err := tx.QueryRow(ctx, `SELECT 1 FROM customers WHERE id = $1`, id).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return badRequest("customer %s does not exist", id)
	}
	return err
}

type candidateRow struct {
	CustomerID string    `json:"customer_id" db:"customer_id"`
	Kind       string    `json:"kind" db:"kind"`
	Value      string    `json:"value" db:"value"`
	AllowCount int64     `json:"allow_count" db:"allow_count"`
	BadCount   int64     `json:"bad_count" db:"bad_count"`
	Status     string    `json:"status" db:"status"`
	FirstSeen  time.Time `json:"first_seen" db:"first_seen"`
	LastSeen   time.Time `json:"last_seen" db:"last_seen"`
	DecidedBy  string    `json:"decided_by" db:"decided_by"`
}

const candidateColumns = `customer_id, kind, value, allow_count, bad_count, status, first_seen, last_seen, decided_by`

func (s *Server) listCandidates(w http.ResponseWriter, r *http.Request) error {
	limit, offset, err := pageParams(r, 100, 1000)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	status := q.Get("status")
	if status == "" {
		status = "pending"
	}
	var f filter
	f.add("status = ?", status)
	if v := q.Get("customer_id"); v != "" {
		f.add("customer_id = ?", v)
	}
	if v := q.Get("kind"); v != "" {
		f.add("kind = ?", v)
	}
	where := f.where()
	items, err := collect[candidateRow](r.Context(), s.pool, `SELECT `+candidateColumns+` FROM wl_candidates`+where+
		` ORDER BY allow_count DESC LIMIT `+f.arg(limit)+` OFFSET `+f.arg(offset), f.args...)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

type decideRequest struct {
	CustomerID string `json:"customer_id"`
	Kind       string `json:"kind"`
	Value      string `json:"value"`
	Global     bool   `json:"global"` // approve onto the global White List instead of the customer's
}

// decideCandidate approves (adds to the White List) or rejects a candidate.
func (s *Server) decideCandidate(w http.ResponseWriter, r *http.Request) error {
	decision := r.PathValue("decision")
	status := map[string]string{"approve": "approved", "reject": "rejected"}[decision]
	if status == "" {
		return notFound("decision")
	}
	var req decideRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	ctx := r.Context()
	me := userFrom(ctx)
	listCustomer := req.CustomerID
	if req.Global {
		listCustomer = ""
	}
	var cand candidateRow
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := collect[candidateRow](ctx, tx, `UPDATE wl_candidates SET status = $4, decided_by = $5
			WHERE customer_id = $1 AND kind = $2 AND value = $3 AND status IN ('pending', 'watching')
			RETURNING `+candidateColumns, req.CustomerID, req.Kind, req.Value, status, me.Username)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return notFound("open candidate")
		}
		cand = rows[0]
		if status == "approved" {
			if _, err := tx.Exec(ctx, `INSERT INTO list_entries (list, kind, value, customer_id, source, note, created_by)
				VALUES ('white', $1, $2, $3, 'wl-collector', $4, $5) ON CONFLICT DO NOTHING`,
				cand.Kind, cand.Value, listCustomer,
				fmt.Sprintf("seen %d× in ALLOW verdicts", cand.AllowCount), me.Username); err != nil {
				return err
			}
		}
		return s.audit(ctx, tx, me, decision, "wl_candidate", cand.CustomerID+"/"+cand.Kind+"/"+cand.Value,
			map[string]any{"global": req.Global})
	})
	if err != nil {
		return err
	}
	ok := true
	if status == "approved" {
		ok = s.refreshed("white list", s.cache.SetMember(ctx, cache.ListKey("white", listCustomer), cand.Value, true))
	}
	return writeJSON(w, http.StatusOK, map[string]any{"candidate": cand, "redis_refreshed": ok})
}
