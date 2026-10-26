// Package consumer reads result events from Kafka in batches. A batch is
// handed to the handler and its offsets are committed only after the
// handler succeeded, so nothing is lost if a service dies mid-batch
// (handlers make redelivery harmless).
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"xrayplatform/internal/app"
	"xrayplatform/internal/config"
	"xrayplatform/internal/model"
)

// Handler stores one batch. Returning an error makes the consumer retry the
// same batch with backoff.
type Handler func(ctx context.Context, events []model.Event) error

// Reader is the part of *kafka.Reader the consumer uses.
type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

func NewReader(cfg config.Kafka) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.Group,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
}

type Consumer struct {
	r          Reader
	batchSize  int
	flushEvery time.Duration
	handle     Handler
	metrics    *app.Metrics
	log        *slog.Logger
	// drainTimeout bounds the last batch after ctx is cancelled.
	drainTimeout time.Duration
	maxBackoff   time.Duration
}

func New(r Reader, batchSize int, flushEvery time.Duration, h Handler, m *app.Metrics, log *slog.Logger) *Consumer {
	return &Consumer{r: r, batchSize: batchSize, flushEvery: flushEvery, handle: h, metrics: m, log: log,
		drainTimeout: 30 * time.Second, maxBackoff: 30 * time.Second}
}

// Run consumes until ctx is cancelled. The batch in hand at that moment is
// still stored and committed.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.r.Close()
	for {
		msgs, err := c.collect(ctx)
		if len(msgs) > 0 {
			c.process(ctx, msgs)
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

// collect blocks for the first message, then gathers more until the batch is
// full or flushEvery has passed.
func (c *Consumer) collect(ctx context.Context) ([]kafka.Message, error) {
	first, err := c.r.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}
	msgs := []kafka.Message{first}
	fctx, cancel := context.WithTimeout(ctx, c.flushEvery)
	defer cancel()
	for len(msgs) < c.batchSize {
		m, err := c.r.FetchMessage(fctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return msgs, ctx.Err()
			}
			return msgs, err
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func (c *Consumer) process(ctx context.Context, msgs []kafka.Message) {
	events := make([]model.Event, 0, len(msgs))
	for _, m := range msgs {
		var e model.Event
		if err := json.Unmarshal(m.Value, &e); err != nil || e.MessageID == "" || !e.Action.Valid() {
			// A bad event can never succeed; skip it rather than block the partition.
			c.metrics.Inc("events_bad")
			c.log.Warn("skipping bad event", "partition", m.Partition, "offset", m.Offset, "err", err)
			continue
		}
		if e.DecidedAt.IsZero() {
			e.DecidedAt = m.Time
		}
		e.DecidedAt = e.DecidedAt.UTC()
		events = append(events, e)
	}

	// After shutdown starts, the batch gets drainTimeout to finish.
	hctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
			select {
			case <-time.After(c.drainTimeout):
				cancel()
			case <-hctx.Done():
			}
		case <-hctx.Done():
		}
	}()

	if len(events) > 0 {
		backoff := 500 * time.Millisecond
		for {
			err := c.handle(hctx, events)
			if err == nil {
				break
			}
			c.metrics.Inc("batch_errors")
			c.log.Warn("batch failed, retrying", "events", len(events), "err", err, "retry_in", backoff)
			select {
			case <-hctx.Done():
				c.log.Warn("giving up on batch at shutdown; it will be redelivered", "events", len(events))
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, c.maxBackoff)
		}
		c.metrics.Add("events_stored", int64(len(events)))
	}
	if err := c.r.CommitMessages(hctx, msgs...); err != nil {
		c.metrics.Inc("commit_errors")
		c.log.Warn("commit failed; batch will be redelivered", "err", err)
		return
	}
	c.metrics.Inc("batches")
	last := msgs[len(msgs)-1]
	c.metrics.Set("lag_seconds", int64(time.Since(last.Time).Seconds()))
}
