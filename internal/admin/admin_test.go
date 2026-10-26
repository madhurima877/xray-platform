package admin

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestRoles(t *testing.T) {
	if !Admin.Allows(Operator) || !Operator.Allows(Viewer) || Viewer.Allows(Operator) || Operator.Allows(Admin) {
		t.Fatal("role order is wrong")
	}
	if Role("root").Valid() || Role("root").Allows(Viewer) {
		t.Fatal("unknown role must have no rights")
	}
}

func TestToken(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Now()
	tok, err := issueToken(secret, User{ID: 42, Username: "kim", Role: Operator}, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := parseToken(secret, tok); err != nil || id != 42 {
		t.Fatalf("parse = %d, %v", id, err)
	}
	if _, err := parseToken([]byte("another-secret-another-secret-xx"), tok); err == nil {
		t.Fatal("token signed with another secret must fail")
	}
	old, _ := issueToken(secret, User{ID: 42}, time.Hour, now.Add(-2*time.Hour))
	if _, err := parseToken(secret, old); err == nil {
		t.Fatal("expired token must fail")
	}
}

func TestNormalize(t *testing.T) {
	if v, err := normalizeValue("url_host", "https://WWW.Bad.example/x"); err != nil || v != "bad.example" {
		t.Fatalf("url_host = %q, %v", v, err)
	}
	if _, err := normalizeValue("url_host", "::"); err == nil {
		t.Fatal("bad host must fail")
	}
	if _, err := normalizeValue("ip", "1.2.3.4"); err == nil {
		t.Fatal("unknown kind must fail")
	}
	if p, err := normalizePhrase("  Free   MONEY "); err != nil || p != "free money" {
		t.Fatalf("phrase = %q, %v", p, err)
	}
}

func TestCustomerPolicy(t *testing.T) {
	base := customerRow{Level: 1, KeywordAction: "HOLD", AIBlock: 0.9, AIHold: 0.7}
	lvl, hold := 5, float32(0.95)
	c := base
	if err := (updateCustomerRequest{Level: &lvl}).apply(&c); err != nil || c.Level != 5 {
		t.Fatalf("level 5: %v", err)
	}
	c = base
	if err := (updateCustomerRequest{AIHold: &hold}).apply(&c); err == nil {
		t.Fatal("ai_hold above ai_block must fail")
	}
	bad := 6
	c = base
	if err := (updateCustomerRequest{Level: &bad}).apply(&c); err == nil {
		t.Fatal("level 6 must fail")
	}
}

func TestFilterAndRange(t *testing.T) {
	var f filter
	f.add("a = ?", 1)
	f.add("(b ILIKE ? OR c ILIKE ?)", "%x%")
	if got := f.where() + " LIMIT " + f.arg(10); got != " WHERE a = $1 AND (b ILIKE $2 OR c ILIKE $2) LIMIT $3" {
		t.Fatalf("got %q", got)
	}
	r := httptest.NewRequest("GET", "/?from=2026-10-02T00:00:00Z&to=2026-10-01T00:00:00Z", nil)
	if _, _, err := timeRange(r, time.Hour*24*400); err == nil {
		t.Fatal("from after to must fail")
	}
	r = httptest.NewRequest("GET", "/", nil)
	from, to, err := timeRange(r, time.Hour*48)
	if err != nil || to.Sub(from) != 24*time.Hour {
		t.Fatalf("default range %s–%s, %v", from, to, err)
	}
}
