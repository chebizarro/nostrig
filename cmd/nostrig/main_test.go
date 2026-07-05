package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestClaimDryRunPrintsTaskClaimEvent(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"claim", "--task-id", "task-1", "--claimer", "agent-a", "--recipient", "recipient-pubkey", "--dry-run"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"\"kind\":25910", "task/claim", "recipient-pubkey", "task-1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("dry-run output missing %q: %s", want, got)
		}
	}
}

func TestRelaysWithEnv(t *testing.T) {
	t.Setenv("NOSTR_RELAY", "wss://one.example,wss://two.example")
	got := relaysWithEnv(nil)
	if len(got) != 2 || got[0] != "wss://one.example" || got[1] != "wss://two.example" {
		t.Fatalf("relays=%#v", got)
	}
}
