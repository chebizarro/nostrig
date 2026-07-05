package taskfabric

import (
	"strings"
	"testing"
	"time"

	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
)

func TestMatchContextVMResponseByEventTag(t *testing.T) {
	cmd, err := nip34.BuildClaimDispatch("task-1", "agent-a", "recipient", time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	cmd.ID = "cmd-event-id"
	respEvent := &gonostr.Event{Kind: nip34.KindContextVMIntent, Tags: gonostr.Tags{{"e", cmd.ID}}, Content: `{"jsonrpc":"2.0","id":"` + jsonRPCID(cmd.Content) + `","result":{"ok":true}}`}
	resp, ok := MatchContextVMResponse(cmd, respEvent)
	if !ok {
		t.Fatal("response did not match")
	}
	if resp.Result == nil || !strings.Contains(string(*resp.Result), "ok") {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestMatchContextVMResponseRejectsUncorrelated(t *testing.T) {
	cmd, err := nip34.BuildClaimDispatch("task-1", "agent-a", "recipient", time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	cmd.ID = "cmd-event-id"
	respEvent := &gonostr.Event{Kind: nip34.KindContextVMIntent, Tags: gonostr.Tags{{"e", "other"}}, Content: `{"jsonrpc":"2.0","id":"other","result":{"ok":true}}`}
	if _, ok := MatchContextVMResponse(cmd, respEvent); ok {
		t.Fatal("uncorrelated response matched")
	}
}
