package kisa

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestParseList(t *testing.T) {
	cases := []struct {
		name string
		body string
		want []string
	}{
		{"text", "# header\nhttp://Evil.example/x\n\nphish.example # note\nwww.evil.example/y\n", []string{"evil.example", "phish.example"}},
		{"json strings", `["https://a.example/1", "b.example"]`, []string{"a.example", "b.example"}},
		{"json objects", `[{"url": "https://c.example/1"}, {"url": "c.example"}]`, []string{"c.example"}},
	}
	for _, c := range cases {
		got, err := ParseList([]byte(c.body))
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, %v; want %v", c.name, got, err, c.want)
		}
	}
	if _, err := ParseList([]byte("# only comments\n")); !errors.Is(err, ErrEmptyList) {
		t.Errorf("empty list: err = %v, want ErrEmptyList", err)
	}
	if _, err := ParseList([]byte(`[1, 2]`)); err == nil {
		t.Error("bad JSON items must fail")
	}
}

func TestBackoff(t *testing.T) {
	for attempt, base := range map[int]time.Duration{1: 5 * time.Second, 2: 10 * time.Second, 4: 40 * time.Second, 30: time.Hour} {
		for i := 0; i < 50; i++ {
			d := Backoff(attempt)
			if d < base*8/10 || d > base*12/10 {
				t.Fatalf("Backoff(%d) = %s, want %s ±20%%", attempt, d, base)
			}
		}
	}
}

func TestPermanent(t *testing.T) {
	for status, want := range map[int]bool{0: false, 400: true, 401: true, 408: false, 429: false, 500: false, 503: false} {
		if got := permanent(status); got != want {
			t.Errorf("permanent(%d) = %v", status, got)
		}
	}
}
