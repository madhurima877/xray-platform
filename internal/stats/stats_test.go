package stats

import (
	"testing"
	"time"

	"xrayplatform/internal/model"
)

func TestAggregate(t *testing.T) {
	t0 := time.Date(2026, 10, 26, 9, 15, 20, 0, time.UTC)
	events := []model.Event{
		{MessageID: "1", CustomerID: "c1", Action: model.Block, DecidedAt: t0, LatencyMS: 10,
			Reasons: []model.Reason{{Step: "blacklist", Action: model.Block}, {Step: "blacklist", Action: model.Block}}},
		{MessageID: "2", CustomerID: "c1", Action: model.Block, DecidedAt: t0.Add(30 * time.Second), LatencyMS: 30,
			Reasons: []model.Reason{{Step: "keywords", Action: model.Block}}},
		{MessageID: "3", CustomerID: "c1", Action: model.Allow, DecidedAt: t0.Add(time.Minute), LatencyMS: 5},
	}
	minutes, reasons := Aggregate(events)

	k := MinuteKey{Customer: "c1", Bucket: time.Date(2026, 10, 26, 9, 15, 0, 0, time.UTC), Verdict: model.Block}
	if c := minutes[k]; c.N != 2 || c.LatencySum != 40 || c.LatencyMax != 30 {
		t.Fatalf("block minute = %+v", c)
	}
	if len(minutes) != 2 {
		t.Fatalf("want 2 minute rows, got %d", len(minutes))
	}
	day := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	if n := reasons[ReasonKey{"c1", day, "blacklist", model.Block}]; n != 1 {
		t.Fatalf("blacklist counted %d times, want 1 (same step twice in one message)", n)
	}
	if n := reasons[ReasonKey{"c1", day, "keywords", model.Block}]; n != 1 {
		t.Fatalf("keywords = %d", n)
	}
}
