package taskfabric

import (
	"context"
	"testing"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExportFromTaskStateEventsLatestWins(t *testing.T) {
	older := time.Unix(100, 0).UTC()
	newer := time.Unix(200, 0).UTC()
	oldIssue := &beadspb.Issue{Id: "repo-task1", Title: "old", Status: beadspb.Status_STATUS_OPEN, Updated: timestamppb.New(older)}
	newIssue := &beadspb.Issue{Id: "repo-task1", Title: "new", Status: beadspb.Status_STATUS_IN_PROGRESS, Updated: timestamppb.New(newer)}
	oldEvent, err := nip34.BuildTaskStateEvent(oldIssue, older)
	if err != nil {
		t.Fatal(err)
	}
	newEvent, err := nip34.BuildTaskStateEvent(newIssue, newer)
	if err != nil {
		t.Fatal(err)
	}
	oldEvent.ID = "old-event"
	oldEvent.PubKey = "old-pubkey"
	newEvent.ID = "new-event"
	newEvent.PubKey = "new-pubkey"

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
	if issue.Metadata.Custom["nostr.id"] != "new-event" {
		t.Fatalf("metadata not updated: %#v", issue.Metadata.Custom)
	}
}

func TestFetchTaskStateEventsRequiresBoundedSelector(t *testing.T) {
	_, err := FetchTaskStateEvents(context.Background(), nil, SyncOptions{Relays: []string{"wss://relay.example"}})
	if err == nil {
		t.Fatal("expected bounded selector error")
	}
}
