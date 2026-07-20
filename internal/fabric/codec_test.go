package fabric

import (
	"testing"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFullBeadsRoundTrip(t *testing.T) {
	now := timestamppb.New(time.Unix(1712345678, 0).UTC())
	want := &beadspb.Export{
		Epics:  []*beadspb.Epic{{Id: "fp-tf", Name: "Task fabric", Description: "Nostr ledger", Status: beadspb.Status_STATUS_IN_PROGRESS, Created: now, Updated: now, Metadata: &beadspb.Metadata{JiraKey: "TF", Custom: map[string]string{"source": "nip34"}, Repositories: []string{"nostrig"}}}},
		Issues: []*beadspb.Issue{{Id: "fp-50", Title: "Lossless fabric", Description: "all fields", Status: beadspb.Status_STATUS_BLOCKED, Priority: beadspb.Priority_PRIORITY_P0, Epic: "fp-tf", Assignee: "abcdef", Labels: []string{"nostr", "critical"}, DependsOn: []string{"fp-2", "fp-4"}, Created: now, Updated: now, Metadata: &beadspb.Metadata{JiraKey: "TF-50", JiraId: "50", JiraIssueType: "Story", Custom: map[string]string{"nostr.id": "deadbeef"}, Repositories: []string{"nostrig", "cascadia-nips"}}}},
	}
	events, err := Encode(want, "0123456789abcdef", time.Unix(1712345680, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events", len(events))
	}
	got, err := Decode(events)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(want, got) {
		t.Fatalf("round trip changed model\nwant: %v\n got: %v", want, got)
	}
}

func TestDecodeRejectsAddressMismatch(t *testing.T) {
	ex := &beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-50"}}}
	events, err := Encode(ex, "pubkey", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	events[0].Tags[0][1] = "task:fp-51"
	if _, err := Decode(events); err == nil {
		t.Fatal("expected mismatch error")
	}
}
