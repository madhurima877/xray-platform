package wl

import (
	"testing"

	"xrayplatform/internal/model"
)

func TestCollect(t *testing.T) {
	events := []model.Event{
		{CustomerID: "c1", Action: model.Allow, Sender: "+911", URLs: []string{"https://www.shop.example/a", "http://shop.example/b"}},
		{CustomerID: "c1", Action: model.Hold, Sender: "+911"},
		{CustomerID: "c1", Action: model.FailOpen, Sender: "+922"},
		{CustomerID: "c1", Action: model.Allow, Sender: "+933",
			Reasons: []model.Reason{{Step: "whitelist", Action: model.Allow}}},
	}
	got := Collect(events)
	if c := got[Item{"c1", "url_host", "shop.example"}]; c.Allow != 1 || c.Bad != 0 {
		t.Fatalf("host counted %+v; two URLs on one host in one message count once", c)
	}
	if c := got[Item{"c1", "sender", "+911"}]; c.Allow != 1 || c.Bad != 1 {
		t.Fatalf("sender = %+v", c)
	}
	if _, ok := got[Item{"c1", "sender", "+922"}]; ok {
		t.Fatal("FAIL_OPEN must be ignored")
	}
	if _, ok := got[Item{"c1", "sender", "+933"}]; ok {
		t.Fatal("already whitelisted messages must be ignored")
	}
}
