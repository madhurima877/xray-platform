// Package mastersync is master-sync: it copies the relay's customer master
// (ID, name, callback URL, active flag, API keys) into PostgreSQL and then
// into the Redis keys xray-api reads. Level and policies stay as set in the
// admin web (S-04); a new customer starts at T1 with default policies.
package mastersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"xrayplatform/internal/cache"
)

// Customer is one entry of the relay's customer list. The field names are
// our assumption until the relay's master-data API spec is final.
type Customer struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	CallbackURL string   `json:"callback_url"`
	Active      bool     `json:"active"`
	APIKeys     []string `json:"api_keys"`
}

type Syncer struct {
	url    string
	token  string
	client *http.Client
	pool   *pgxpool.Pool
	cache  *cache.Cache
}

func New(url, token string, client *http.Client, pool *pgxpool.Pool, c *cache.Cache) *Syncer {
	return &Syncer{url: url, token: token, client: client, pool: pool, cache: c}
}

func (s *Syncer) Run(ctx context.Context) (int, error) {
	list, err := s.fetch(ctx)
	if err != nil {
		return 0, err
	}
	if err := Apply(ctx, s.pool, list); err != nil {
		return 0, err
	}
	if _, err := s.cache.SyncCustomers(ctx); err != nil {
		return 0, fmt.Errorf("refresh redis: %w", err)
	}
	return len(list), nil
}

func (s *Syncer) fetch(ctx context.Context) ([]Customer, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil, err
	}
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("relay customer list answered %d", resp.StatusCode)
	}
	var body struct {
		Customers []Customer `json:"customers"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(&body); err != nil {
		return nil, fmt.Errorf("parse relay customer list: %w", err)
	}
	return Validate(body.Customers)
}

// Validate drops nothing silently: a customer without an ID or a duplicate
// ID fails the whole run, so a bad export never half-applies.
func Validate(list []Customer) ([]Customer, error) {
	if len(list) == 0 {
		return nil, errors.New("relay returned no customers; keeping the current copy")
	}
	seen := map[string]bool{}
	for i, c := range list {
		if c.ID == "" {
			return nil, fmt.Errorf("customer #%d has no id", i)
		}
		if seen[c.ID] {
			return nil, fmt.Errorf("customer %s appears twice", c.ID)
		}
		seen[c.ID] = true
	}
	return list, nil
}

func HashKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}

// Apply upserts the list. Customers missing from it are deactivated, not
// deleted, so their history and settings survive.
func Apply(ctx context.Context, pool *pgxpool.Pool, list []Customer) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		ids := make([]string, len(list))
		b := &pgx.Batch{}
		for i, c := range list {
			ids[i] = c.ID
			b.Queue(`INSERT INTO customers (id, name, callback_url, active, synced_at) VALUES ($1, $2, $3, $4, now())
				ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, callback_url = EXCLUDED.callback_url,
					active = EXCLUDED.active, synced_at = now(), updated_at = now()`,
				c.ID, c.Name, c.CallbackURL, c.Active)
		}
		b.Queue(`UPDATE customers SET active = false, synced_at = now(), updated_at = now()
			WHERE active AND NOT (id = ANY($1::text[]))`, ids)
		b.Queue(`DELETE FROM api_keys WHERE customer_id = ANY($1::text[])`, ids)
		for _, c := range list {
			for _, k := range c.APIKeys {
				if k == "" {
					continue
				}
				// A key moved to another customer follows the latest export.
				b.Queue(`INSERT INTO api_keys (key_hash, customer_id) VALUES ($1, $2)
					ON CONFLICT (key_hash) DO UPDATE SET customer_id = EXCLUDED.customer_id`, HashKey(k), c.ID)
			}
		}
		return tx.SendBatch(ctx, b).Close()
	})
}
