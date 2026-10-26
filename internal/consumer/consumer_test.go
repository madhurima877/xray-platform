package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"xrayplatform/internal/app"
	"xrayplatform/internal/model"
)

type fakeReader struct {
	mu        sync.Mutex
	msgs      chan kafka.Message
	committed []int64
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case m := <-f.msgs:
		return m, nil
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	}
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range msgs {
		f.committed = append(f.committed, m.Offset)
	}
	return nil
}

func (f *fakeReader) Close() error { return nil }

func (f *fakeReader) commits() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.committed...)
}

func event(t *testing.T, id string, offset int64) kafka.Message {
	b, err := json.Marshal(model.Event{MessageID: id, CustomerID: "c1", Action: model.Allow, DecidedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Value: b, Offset: offset, Time: time.Now()}
}

func newTest(r Reader, h Handler) *Consumer {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := New(r, 3, 50*time.Millisecond, h, app.NewMetrics("test"), log)
	c.maxBackoff = 10 * time.Millisecond
	return c
}

// A failing batch is retried and committed only after it succeeds; a bad
// message is skipped but still committed.
func TestRetryThenCommit(t *testing.T) {
	r := &fakeReader{msgs: make(chan kafka.Message, 10)}
	r.msgs <- event(t, "m1", 1)
	r.msgs <- kafka.Message{Value: []byte("not json"), Offset: 2}
	r.msgs <- event(t, "m3", 3)

	var mu sync.Mutex
	calls := 0
	var got []string
	ctx, cancel := context.WithCancel(context.Background())
	h := func(_ context.Context, evs []model.Event) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls < 3 {
			return errors.New("db down")
		}
		for _, e := range evs {
			got = append(got, e.MessageID)
		}
		cancel()
		return nil
	}
	if err := newTest(r, h).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("handler called %d times, want 3", calls)
	}
	if len(got) != 2 || got[0] != "m1" || got[1] != "m3" {
		t.Fatalf("handled %v", got)
	}
	if c := r.commits(); len(c) != 3 {
		t.Fatalf("committed %v, want offsets 1–3", c)
	}
}

// A partial batch is flushed after flushEvery, and nothing is committed
// while the handler keeps failing.
func TestNoCommitWhileFailing(t *testing.T) {
	r := &fakeReader{msgs: make(chan kafka.Message, 10)}
	r.msgs <- event(t, "m1", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c := newTest(r, func(context.Context, []model.Event) error { return errors.New("db down") })
	c.drainTimeout = 50 * time.Millisecond
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got := r.commits(); len(got) != 0 {
		t.Fatalf("committed %v while the handler failed", got)
	}
}
