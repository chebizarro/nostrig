package taskfabric

import (
	"testing"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMergeTaskStateDetectsLocalRelayConflict(t *testing.T) {
	base := &TaskSnapshot{ID: "task-1", Title: "base", Status: "open", Updated: "2026-01-01T00:00:00Z"}
	prev := &CacheRecord{ID: "task-1", Resolved: base, Local: base, Relay: base, LocalRevision: SnapshotRevision(base), RelayEventID: "relay-old", Resolution: ResolutionClean}
	local := &TaskSnapshot{ID: "task-1", Title: "local title", Status: "open", Updated: "2026-01-02T00:00:00Z"}
	relayIssue := issue("task-1", "relay title", "relay-new", time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC))

	merged, err := MergeTaskState(&beadspb.Export{Issues: []*beadspb.Issue{relayIssue}}, []*TaskSnapshot{local}, []*CacheRecord{prev})
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.Conflicts) != 1 {
		t.Fatalf("conflicts=%d want 1", len(merged.Conflicts))
	}
	rec := merged.Conflicts[0]
	if rec.Resolution != ResolutionConflict || rec.Conflict == nil {
		t.Fatalf("record not marked conflict: %#v", rec)
	}
	if rec.Conflict.ChangedFields[0] != "title" {
		t.Fatalf("changed fields=%#v", rec.Conflict.ChangedFields)
	}
	if rec.Resolved.Title != "base" {
		t.Fatalf("conflict should preserve previous resolved snapshot, got %q", rec.Resolved.Title)
	}
}

func TestMergeTaskStateAutoMergesCompatibleStatusByLatest(t *testing.T) {
	base := &TaskSnapshot{ID: "task-1", Title: "same", Status: "open", Updated: "2026-01-01T00:00:00Z"}
	prev := &CacheRecord{ID: "task-1", Resolved: base, Local: base, Relay: base, LocalRevision: SnapshotRevision(base), RelayEventID: "relay-old", Resolution: ResolutionClean}
	local := &TaskSnapshot{ID: "task-1", Title: "same", Status: "in_progress", Updated: "2026-01-04T00:00:00Z"}
	relayIssue := issue("task-1", "same", "relay-new", time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC))
	relayIssue.Status = beadspb.Status_STATUS_OPEN

	merged, err := MergeTaskState(&beadspb.Export{Issues: []*beadspb.Issue{relayIssue}}, []*TaskSnapshot{local}, []*CacheRecord{prev})
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.Conflicts) != 0 {
		t.Fatalf("conflicts=%d want 0", len(merged.Conflicts))
	}
	if merged.Records[0].Resolution != ResolutionLatestWins || merged.Records[0].Resolved.Status != "in_progress" {
		t.Fatalf("latest compatible status did not win: %#v", merged.Records[0])
	}
}

func issue(id, title, eventID string, updated time.Time) *beadspb.Issue {
	return &beadspb.Issue{Id: id, Title: title, Status: beadspb.Status_STATUS_OPEN, Updated: timestamppb.New(updated), Metadata: &beadspb.Metadata{Custom: map[string]string{"nostr.id": eventID}}}
}
