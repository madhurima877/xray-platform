// Package cache rebuilds the Redis keys xray-api reads from PostgreSQL.
// The key layout must match xray/internal/store:
//
//	apikey:{sha256(key)}  string  → customer ID
//	customer:{id}         hash    level, rate_limit, callback_url, keyword_action, ai_block, ai_hold
//	bl, wl                set     global Black/White List
//	bl:{id}, wl:{id}      set     per-customer Black/White List
//	keywords              set     keyword phrases
//
// Sets are filled under a temporary key and swapped in with RENAME, so
// xray-api never sees a half-built list.
package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	Prefix = "xray:"
	chunk  = 1000
)

type Cache struct {
	rdb  *redis.Client
	pool *pgxpool.Pool
}

func New(rdb *redis.Client, pool *pgxpool.Pool) *Cache { return &Cache{rdb: rdb, pool: pool} }

// ListKey is the Redis set for a list ("black"/"white") and customer ("" = global).
func ListKey(list, customerID string) string {
	k := Prefix + "wl"
	if list == "black" {
		k = Prefix + "bl"
	}
	if customerID != "" {
		k += ":" + customerID
	}
	return k
}

// SyncLists rebuilds every Black/White List set and deletes sets that no
// longer have entries.
func (c *Cache) SyncLists(ctx context.Context) (int, error) {
	rows, err := c.pool.Query(ctx, `SELECT list, customer_id, value FROM list_entries`)
	if err != nil {
		return 0, err
	}
	sets := map[string][]string{}
	n := 0
	for rows.Next() {
		var list, cust, value string
		if err := rows.Scan(&list, &cust, &value); err != nil {
			rows.Close()
			return 0, err
		}
		k := ListKey(list, cust)
		sets[k] = append(sets[k], value)
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	var existing []string
	for _, pattern := range []string{Prefix + "bl", Prefix + "bl:*", Prefix + "wl", Prefix + "wl:*"} {
		keys, err := c.scan(ctx, pattern)
		if err != nil {
			return 0, err
		}
		existing = append(existing, keys...)
	}
	for k, members := range sets {
		if err := c.replaceSet(ctx, k, members); err != nil {
			return 0, err
		}
	}
	for _, k := range existing {
		if _, keep := sets[k]; !keep {
			if err := c.rdb.Del(ctx, k).Err(); err != nil {
				return 0, err
			}
		}
	}
	return n, nil
}

// SyncKeywords rebuilds the keyword set from the active keywords.
func (c *Cache) SyncKeywords(ctx context.Context) (int, error) {
	rows, err := c.pool.Query(ctx, `SELECT phrase FROM keywords WHERE active ORDER BY phrase`)
	if err != nil {
		return 0, err
	}
	var phrases []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return 0, err
		}
		phrases = append(phrases, p)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return len(phrases), c.replaceSet(ctx, Prefix+"keywords", phrases)
}

// SyncCustomers writes the customer hash and API keys of every active
// customer and removes inactive customers and unknown keys.
func (c *Cache) SyncCustomers(ctx context.Context) (int, error) {
	rows, err := c.pool.Query(ctx, `SELECT id, active, level, rate_limit, callback_url, keyword_action, ai_block, ai_hold
		FROM customers`)
	if err != nil {
		return 0, err
	}
	pipe := c.rdb.Pipeline()
	active := 0
	for rows.Next() {
		var (
			id, callback, kwAction string
			on                     bool
			level, rateLimit       int
			aiBlock, aiHold        float32
		)
		if err := rows.Scan(&id, &on, &level, &rateLimit, &callback, &kwAction, &aiBlock, &aiHold); err != nil {
			rows.Close()
			return 0, err
		}
		key := Prefix + "customer:" + id
		if !on {
			pipe.Del(ctx, key)
			continue
		}
		active++
		pipe.HSet(ctx, key,
			"level", level,
			"rate_limit", rateLimit,
			"callback_url", callback,
			"keyword_action", kwAction,
			"ai_block", strconv.FormatFloat(float64(aiBlock), 'f', 4, 32),
			"ai_hold", strconv.FormatFloat(float64(aiHold), 'f', 4, 32))
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}

	keyRows, err := c.pool.Query(ctx, `SELECT k.key_hash, k.customer_id FROM api_keys k
		JOIN customers c ON c.id = k.customer_id WHERE c.active`)
	if err != nil {
		return 0, err
	}
	want := map[string]string{}
	for keyRows.Next() {
		var hash, cust string
		if err := keyRows.Scan(&hash, &cust); err != nil {
			keyRows.Close()
			return 0, err
		}
		want[Prefix+"apikey:"+hash] = cust
	}
	if err := keyRows.Err(); err != nil {
		return 0, err
	}
	existing, err := c.scan(ctx, Prefix+"apikey:*")
	if err != nil {
		return 0, err
	}
	pipe = c.rdb.Pipeline()
	for k, cust := range want {
		pipe.Set(ctx, k, cust, 0)
	}
	for _, k := range existing {
		if _, ok := want[k]; !ok {
			pipe.Del(ctx, k)
		}
	}
	if pipe.Len() > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, err
		}
	}
	return active, nil
}

// SetMember adds or removes one list entry; admin-api uses it so a single
// edit does not rebuild a list that may hold the whole KISA feed.
func (c *Cache) SetMember(ctx context.Context, key, value string, present bool) error {
	if present {
		return c.rdb.SAdd(ctx, key, value).Err()
	}
	return c.rdb.SRem(ctx, key, value).Err()
}

// SyncCustomer rewrites one customer's hash after an edit on S-04.
func (c *Cache) SyncCustomer(ctx context.Context, id string) error {
	var (
		callback, kwAction string
		on                 bool
		level, rateLimit   int
		aiBlock, aiHold    float32
	)
	err := c.pool.QueryRow(ctx, `SELECT active, level, rate_limit, callback_url, keyword_action, ai_block, ai_hold
		FROM customers WHERE id = $1`, id).Scan(&on, &level, &rateLimit, &callback, &kwAction, &aiBlock, &aiHold)
	if err != nil {
		return err
	}
	key := Prefix + "customer:" + id
	if !on {
		return c.rdb.Del(ctx, key).Err()
	}
	return c.rdb.HSet(ctx, key,
		"level", level,
		"rate_limit", rateLimit,
		"callback_url", callback,
		"keyword_action", kwAction,
		"ai_block", strconv.FormatFloat(float64(aiBlock), 'f', 4, 32),
		"ai_hold", strconv.FormatFloat(float64(aiHold), 'f', 4, 32)).Err()
}

func (c *Cache) replaceSet(ctx context.Context, key string, members []string) error {
	if len(members) == 0 {
		return c.rdb.Del(ctx, key).Err()
	}
	tmp := Prefix + "tmp:" + nonce()
	for i := 0; i < len(members); i += chunk {
		end := min(i+chunk, len(members))
		args := make([]any, 0, end-i)
		for _, m := range members[i:end] {
			args = append(args, m)
		}
		if err := c.rdb.SAdd(ctx, tmp, args...).Err(); err != nil {
			c.rdb.Del(ctx, tmp)
			return err
		}
	}
	if err := c.rdb.Rename(ctx, tmp, key).Err(); err != nil {
		c.rdb.Del(ctx, tmp)
		return fmt.Errorf("swap %s: %w", key, err)
	}
	return nil
}

func (c *Cache) scan(ctx context.Context, pattern string) ([]string, error) {
	var out []string
	iter := c.rdb.Scan(ctx, 0, pattern, 1000).Iterator()
	for iter.Next(ctx) {
		out = append(out, iter.Val())
	}
	return out, iter.Err()
}

func nonce() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
