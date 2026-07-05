package nostr

import (
	"testing"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTaskStateRoundTripLossless(t *testing.T) {
	now := time.Unix(1234, 0).UTC()
	issue := &beadspb.Issue{Id: "repo-abc12345", Title: "Implement fabric", Description: "full model", Status: beadspb.Status_STATUS_IN_PROGRESS, Priority: beadspb.Priority_PRIORITY_P1, Epic: "repo-epic1234", Assignee: "agent-a", Labels: []string{"issue", "fabric"}, DependsOn: []string{"repo-dep00001"}, Created: timestamppb.New(now), Updated: timestamppb.New(now.Add(time.Minute)), Metadata: &beadspb.Metadata{Custom: map[string]string{"nostr.id": "root-event", "nip34.repo_addr": "30617:pub:repo", "nostrig.beads_id": "repo-abc12345", "nostrig.id_format": "spec"}}}
	ev, err := BuildTaskStateEvent(issue, now)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != KindCanonicalState {
		t.Fatalf("kind=%d", ev.Kind)
	}
	d, ok := TagD(ev)
	if !ok || d != "task:repo-abc12345" {
		t.Fatalf("d tag=%q ok=%v", d, ok)
	}
	got, err := ParseTaskStateEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if got.Id != issue.Id || got.Title != issue.Title || got.Description != issue.Description || got.Status != issue.Status || got.Priority != issue.Priority || got.Epic != issue.Epic || got.Assignee != issue.Assignee {
		t.Fatalf("round trip mismatch: got %#v want %#v", got, issue)
	}
	if len(got.Labels) != 2 || got.Labels[1] != "fabric" {
		t.Fatalf("labels mismatch: %#v", got.Labels)
	}
	if len(got.DependsOn) != 1 || got.DependsOn[0] != "repo-dep00001" {
		t.Fatalf("deps mismatch: %#v", got.DependsOn)
	}
	if got.Metadata.Custom["nostrig.beads_id"] != issue.Id {
		t.Fatalf("metadata not preserved: %#v", got.Metadata.Custom)
	}
}

func TestBuildContextVMCommand(t *testing.T) {
	ev, err := BuildClaimDispatch("task-1", "agent-a", "recipient-pubkey", time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != KindContextVMIntent {
		t.Fatalf("kind=%d", ev.Kind)
	}
	if method, _ := TagFirst(ev, "method"); method != "task/claim" {
		t.Fatalf("method=%q", method)
	}
}
