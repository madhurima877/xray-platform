package kisa

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"xrayplatform/internal/cache"
	"xrayplatform/internal/hosts"
)

const maxListBytes = 64 << 20

// ErrEmptyList stops a broken download from wiping the Black List.
var ErrEmptyList = errors.New("KISA list is empty; keeping the current Black List")

type Collector struct {
	url    string
	apiKey string
	client *http.Client
	pool   *pgxpool.Pool
	cache  *cache.Cache
}

func NewCollector(url, apiKey string, client *http.Client, pool *pgxpool.Pool, c *cache.Cache) *Collector {
	return &Collector{url: url, apiKey: apiKey, client: client, pool: pool, cache: c}
}

// Run is one job run: download, replace the KISA rows of the global Black
// List, refresh Redis.
func (c *Collector) Run(ctx context.Context) (int, error) {
	list, err := c.fetch(ctx)
	if err != nil {
		return 0, err
	}
	if _, _, err := Apply(ctx, c.pool, list); err != nil {
		return 0, err
	}
	if _, err := c.cache.SyncLists(ctx); err != nil {
		return 0, fmt.Errorf("refresh redis: %w", err)
	}
	return len(list), nil
}

func (c *Collector) fetch(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, err
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("KISA list answered %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxListBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxListBytes {
		return nil, fmt.Errorf("KISA list is larger than %d bytes", maxListBytes)
	}
	return ParseList(body)
}

// ParseList reads the bad-URL list and returns unique list keys (hosts).
// It accepts a JSON array of strings, a JSON array of {"url": ...} objects,
// or plain text with one URL per line ("#" starts a comment).
func ParseList(body []byte) ([]string, error) {
	var raw []string
	trimmed := bytes.TrimSpace(body)
	if bytes.HasPrefix(trimmed, []byte("[")) {
		var items []json.RawMessage
		if err := json.Unmarshal(trimmed, &items); err != nil {
			return nil, fmt.Errorf("parse KISA list: %w", err)
		}
		for _, it := range items {
			var s string
			if json.Unmarshal(it, &s) == nil {
				raw = append(raw, s)
				continue
			}
			var obj struct {
				URL string `json:"url"`
			}
			if err := json.Unmarshal(it, &obj); err != nil {
				return nil, fmt.Errorf("parse KISA list item %s: %w", it, err)
			}
			raw = append(raw, obj.URL)
		}
	} else {
		sc := bufio.NewScanner(bytes.NewReader(trimmed))
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if i := strings.IndexByte(line, '#'); i >= 0 {
				line = strings.TrimSpace(line[:i])
			}
			if line != "" {
				raw = append(raw, line)
			}
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if h := hosts.Normalize(r); h != "" && !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	if len(out) == 0 {
		return nil, ErrEmptyList
	}
	return out, nil
}

// Apply makes the KISA rows of the global Black List equal to list. Entries
// added by operators are left alone.
func Apply(ctx context.Context, pool *pgxpool.Pool, list []string) (added, removed int64, err error) {
	if len(list) == 0 {
		return 0, 0, ErrEmptyList
	}
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO list_entries (list, kind, value, customer_id, source, created_by)
			SELECT 'black', 'url_host', v, '', 'kisa', 'kisa-collector' FROM unnest($1::text[]) AS v
			ON CONFLICT (list, customer_id, value) DO NOTHING`, list)
		if err != nil {
			return err
		}
		added = tag.RowsAffected()
		tag, err = tx.Exec(ctx, `DELETE FROM list_entries
			WHERE source = 'kisa' AND list = 'black' AND customer_id = '' AND NOT (value = ANY($1::text[]))`, list)
		removed = tag.RowsAffected()
		return err
	})
	return added, removed, err
}
