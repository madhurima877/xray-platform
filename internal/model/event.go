// Package model is the result event contract published by xray-api (step 12)
// on topic xray.verdicts. It mirrors xray/internal/model.Event; keep the two
// in step.
package model

import (
	"strings"
	"time"
)

type Action string

const (
	Allow    Action = "ALLOW"
	Hold     Action = "HOLD"
	Block    Action = "BLOCK"
	FailOpen Action = "FAIL_OPEN"
)

func (a Action) Valid() bool {
	switch a {
	case Allow, Hold, Block, FailOpen:
		return true
	}
	return false
}

type Reason struct {
	Step   string `json:"step"`
	Action Action `json:"action"`
	Detail string `json:"detail"`
}

type Event struct {
	MessageID    string    `json:"message_id"`
	CustomerID   string    `json:"customer_id"`
	Action       Action    `json:"verdict"`
	Level        int       `json:"level"`
	Reasons      []Reason  `json:"reasons"`
	AIScore      *float64  `json:"ai_score,omitempty"`
	ModelVersion string    `json:"model_version,omitempty"`
	LatencyMS    float64   `json:"latency_ms"`
	DecidedAt    time.Time `json:"decided_at"`
	Sender       string    `json:"sender,omitempty"`
	URLs         []string  `json:"urls,omitempty"`
}

// Key identifies one message. The relay's message IDs are only unique per
// customer, so the customer is part of it.
func (e Event) Key() string { return e.CustomerID + ":" + e.MessageID }

// HasStep reports whether any reason came from step.
func (e Event) HasStep(step string) bool {
	for _, r := range e.Reasons {
		if strings.EqualFold(r.Step, step) {
			return true
		}
	}
	return false
}
