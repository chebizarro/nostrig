package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestCreateDryRunPrintsFullTaskCreateEvent(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"create", "--task-id", "task-1", "--title", "Build it", "--repo-addr", "30617:owner:repo", "--description", "body", "--priority", "P1", "--epic", "epic-1", "--label", "bug", "--depends-on", "task-0", "--recipient", "recipient-pubkey", "--dry-run"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"\"kind\":25910", "task/create", "recipient-pubkey", "task-1", "Build it", "30617:owner:repo", "P1", "epic-1", "bug", "task-0"} {
		if !strings.Contains(got, want) {
			t.Fatalf("dry-run output missing %q: %s", want, got)
		}
	}
}

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

func TestAssignDryRunPrintsTaskAssignEvent(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"assign", "--task-id", "task-1", "--assignee", "agent-b", "--recipient", "recipient-pubkey", "--dry-run"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"\"kind\":25910", "task/assign", "recipient-pubkey", "agent-b", "task-1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("dry-run output missing %q: %s", want, got)
		}
	}
}

func TestUpdateDryRunPrintsTaskUpdateEvent(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"update", "--task-id", "task-1", "--recipient", "recipient-pubkey", "--status", "in_progress", "--priority", "P0", "--set-label", "urgent", "--add-dep", "task-0", "--dry-run"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"\"kind\":25910", "task/update", "recipient-pubkey", "in_progress", "P0", "urgent", "task-0"} {
		if !strings.Contains(got, want) {
			t.Fatalf("dry-run output missing %q: %s", want, got)
		}
	}
}

func TestDeleteDryRunPrintsTaskDeleteEvent(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"delete", "--task-id", "task-1", "--recipient", "recipient-pubkey", "--dry-run"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"\"kind\":25910", "task/delete", "recipient-pubkey", "task-1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("dry-run output missing %q: %s", want, got)
		}
	}
}

func TestSignerFromOptionsProductionForbidsRawKey(t *testing.T) {
	t.Setenv("NOSTRIG_ENV", "production")
	_, _, err := signerFromOptions(context.Background(), signingOptions{privateKey: "abc123"}, true)
	if err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("expected raw key forbidden error, got %v", err)
	}
}

func TestSignerFromOptionsProductionRequiresBunker(t *testing.T) {
	t.Setenv("NOSTRIG_ENV", "production")
	_, _, err := signerFromOptions(context.Background(), signingOptions{}, true)
	if err == nil || !strings.Contains(err.Error(), "--signer-bunker-url") {
		t.Fatalf("expected signer bunker requirement, got %v", err)
	}
}

func TestRelaysWithEnv(t *testing.T) {
	t.Setenv("NOSTR_RELAY", "wss://one.example,wss://two.example")
	got := relaysWithEnv(nil)
	if len(got) != 2 || got[0] != "wss://one.example" || got[1] != "wss://two.example" {
		t.Fatalf("relays=%#v", got)
	}
}
