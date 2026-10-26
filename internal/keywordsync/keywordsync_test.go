package keywordsync

import (
	"testing"
	"time"
)

func TestDue(t *testing.T) {
	now := time.Now()
	recent, old := now.Add(-time.Hour), now.Add(-25*time.Hour)
	cases := []struct {
		version, pushed int64
		at              *time.Time
		want            bool
	}{
		{2, 2, &recent, false},
		{3, 2, &recent, true}, // edited since last push
		{2, 2, nil, true},     // never pushed
		{2, 2, &old, true},    // daily full push
	}
	for _, c := range cases {
		if got := Due(c.version, c.pushed, c.at, now, 24*time.Hour); got != c.want {
			t.Errorf("Due(%d, %d, %v) = %v", c.version, c.pushed, c.at, got)
		}
	}
}
