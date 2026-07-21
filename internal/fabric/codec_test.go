package fabric

import (
	"fmt"
	"testing"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	gonostr "github.com/nbd-wtf/go-nostr"
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

func TestDecodeLatestWinsAndReplayIsIdempotent(t *testing.T) {
	pub := "author"
	old, _ := Encode(&beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-50", Title: "old"}}}, pub, time.Unix(10, 0))
	newer, _ := Encode(&beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-50", Title: "new"}}}, pub, time.Unix(20, 0))
	got, err := Decode([]*gonostr.Event{newer[0], old[0], newer[0]})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Issues) != 1 || got.Issues[0].Title != "new" {
		t.Fatalf("latest-wins replay failed: %v", got.Issues)
	}
}

func TestDecodeTombstoneAndAuthorIsolation(t *testing.T) {
	pub := "author"
	events, _ := Encode(&beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-50"}}}, pub, time.Unix(10, 0))
	foreignDelete := &gonostr.Event{PubKey: "attacker", CreatedAt: 30, Kind: gonostr.KindDeletion,
		Tags: gonostr.Tags{{"a", fmt.Sprintf("30900:%s:task:fp-50", pub)}}}
	delete := &gonostr.Event{PubKey: pub, CreatedAt: 20, Kind: gonostr.KindDeletion,
		Tags: gonostr.Tags{{"a", fmt.Sprintf("30900:%s:task:fp-50", pub)}}}

	got, err := Decode([]*gonostr.Event{events[0], foreignDelete})
	if err != nil || len(got.Issues) != 1 {
		t.Fatalf("foreign deletion affected task: issues=%v err=%v", got.Issues, err)
	}
	got, err = Decode([]*gonostr.Event{delete, events[0]})
	if err != nil || len(got.Issues) != 0 {
		t.Fatalf("authorized tombstone not applied: issues=%v err=%v", got.Issues, err)
	}
}

func TestDecodeRejectsMalformedCanonicalEvent(t *testing.T) {
	_, err := Decode([]*gonostr.Event{{Kind: 30900, Content: `{}`}})
	if err == nil {
		t.Fatal("expected missing d tag error")
	}
	events, _ := Encode(&beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-50"}}}, "author", time.Now())
	events[0].Content = `{"schema":"wrong","issue":{}}`
	if _, err := Decode(events); err == nil {
		t.Fatal("expected unsupported schema error")
	}
}

func TestDecodeVerifiedRejectsTamperAndFiltersAuthor(t *testing.T) {
	secret := gonostr.GeneratePrivateKey()
	pub, err := gonostr.GetPublicKey(secret)
	if err != nil {
		t.Fatal(err)
	}
	events, _ := Encode(&beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-50"}}}, pub, time.Unix(20, 0))
	if err := events[0].Sign(secret); err != nil {
		t.Fatal(err)
	}
	foreign := *events[0]
	foreign.PubKey = "foreign"
	got, err := DecodeVerified([]*gonostr.Event{events[0], &foreign}, pub)
	if err != nil || len(got.Issues) != 1 {
		t.Fatalf("verified decode failed: issues=%v err=%v", got.Issues, err)
	}
	tampered := *events[0]
	tampered.Content += " "
	if _, err := DecodeVerified([]*gonostr.Event{&tampered}, pub); err == nil {
		t.Fatal("expected tampered signature rejection")
	}
}
