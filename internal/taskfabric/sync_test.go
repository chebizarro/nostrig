package taskfabric

import (
	"context"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExportFromTaskStateEventsLatestWins(t *testing.T) {
	older := time.Unix(100, 0).UTC()
	newer := time.Unix(200, 0).UTC()
	oldIssue := &beadspb.Issue{Id: "repo-task1", Title: "old", Status: beadspb.Status_STATUS_OPEN, Updated: timestamppb.New(older)}
	newIssue := &beadspb.Issue{Id: "repo-task1", Title: "new", Status: beadspb.Status_STATUS_IN_PROGRESS, Updated: timestamppb.New(newer)}
	author := testPubKey(20).Hex()
	oldEvent, err := nip34.BuildTaskStateEvent(oldIssue, author, older)
	if err != nil {
		t.Fatal(err)
	}
	newEvent, err := nip34.BuildTaskStateEvent(newIssue, author, newer)
	if err != nil {
		t.Fatal(err)
	}
	oldEvent.ID = testID(20)
	oldEvent.PubKey = testPubKey(20)
	newEvent.ID = testID(21)
	newEvent.PubKey = testPubKey(20)

	export, err := ExportFromTaskStateEvents([]*gonostr.Event{oldEvent, newEvent})
	if err != nil {
		t.Fatal(err)
	}
	if len(export.Issues) != 1 {
		t.Fatalf("issues=%d", len(export.Issues))
	}
	issue := export.Issues[0]
	if issue.Title != "new" || issue.Status != beadspb.Status_STATUS_IN_PROGRESS {
		t.Fatalf("latest issue not selected: %#v", issue)
	}
	if issue.Metadata.Custom["nostr.id"] != testID(21).Hex() {
		t.Fatalf("metadata not updated: %#v", issue.Metadata.Custom)
	}
}

func TestFetchTaskStateEventsRequiresBoundedSelector(t *testing.T) {
	_, err := FetchTaskStateEvents(context.Background(), nil, SyncOptions{Relays: []string{"wss://relay.example"}})
	if err == nil {
		t.Fatal("expected bounded selector error")
	}
}

func TestPublishWriteBackPublishesNIP34StatusForLinkedClosedTask(t *testing.T) {
	pub := &statusCapturePublisher{}
	issue := &beadspb.Issue{Id: "repo-task1", Title: "Close me", Status: beadspb.Status_STATUS_CLOSED, Metadata: &beadspb.Metadata{Custom: map[string]string{"nostr.id": "root-event", "nip34.repo_addr": "30617:owner:repo"}}}
	result := &MergeResult{Records: []*CacheRecord{{Resolved: SnapshotFromIssue(issue), Local: SnapshotFromIssue(issue), LocalRevision: "rev1", Resolution: ResolutionClean}}}
	count, err := publishWriteBack(context.Background(), SyncOptions{Push: true, SyncNIP34Status: true, Relays: []string{"wss://relay.example"}, Authors: []string{testPubKey(20).Hex()}, Signer: noopSigner{}, Publisher: pub}, result)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("published count=%d, want 2", count)
	}
	if len(pub.events) != 2 {
		t.Fatalf("events=%d, want 2", len(pub.events))
	}
	if pub.events[1].Kind != nip34.KindStatusClosed {
		t.Fatalf("status kind=%d, want %d", pub.events[1].Kind, nip34.KindStatusClosed)
	}
	if got, _ := nip34.TagFirst(pub.events[1], "a"); got != "30617:owner:repo" {
		t.Fatalf("status repo tag=%q", got)
	}
	if got, _ := nip34.TagFirst(pub.events[1], "e"); got != "root-event" {
		t.Fatalf("status root tag=%q", got)
	}
}

type statusCapturePublisher struct{ events []*gonostr.Event }

func (p *statusCapturePublisher) Publish(ctx context.Context, relays []string, signer nip34.Signer, events []*gonostr.Event) error {
	p.events = append(p.events, events...)
	return nil
}
