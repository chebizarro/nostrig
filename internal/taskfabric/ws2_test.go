package taskfabric

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type capturePublisher struct{ events []*gonostr.Event }

func (p *capturePublisher) Publish(ctx context.Context, relays []string, signer nip34.Signer, events []*gonostr.Event) error {
	p.events = append(p.events, events...)
	return nil
}

type noopSigner struct{}

func (noopSigner) SignEvent(ctx context.Context, ev *gonostr.Event) error { return nil }

func TestWriteBackPublishesChangedLocalTaskCanonicalState(t *testing.T) {
	local := &TaskSnapshot{ID: "task-1", Title: "local", Status: "open", Updated: "2026-01-02T00:00:00Z"}
	merged := &MergeResult{Records: []*CacheRecord{{ID: "task-1", Resolved: local, Local: local, Resolution: ResolutionLocalOnly, LocalRevision: SnapshotRevision(local)}}}
	pub := &capturePublisher{}
	count, err := publishWriteBack(context.Background(), SyncOptions{Push: true, Authors: []string{testPubKey(1).Hex()}, Signer: noopSigner{}, Publisher: pub, Relays: []string{"wss://relay.example"}}, merged)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(pub.events) != 1 {
		t.Fatalf("published count=%d events=%d", count, len(pub.events))
	}
	pub.events[0].PubKey = testPubKey(1)
	issue, err := nip34.ParseTaskStateEvent(pub.events[0])
	if err != nil {
		t.Fatal(err)
	}
	if issue.Id != "task-1" || issue.Title != "local" {
		t.Fatalf("unexpected published issue: %#v", issue)
	}
}

func TestMergeTaskStateRelayWinsConflictReconciliation(t *testing.T) {
	base := &TaskSnapshot{ID: "task-1", Title: "base", Status: "open", Updated: "2026-01-01T00:00:00Z"}
	prev := &CacheRecord{ID: "task-1", Resolved: base, Local: base, Relay: base, LocalRevision: SnapshotRevision(base), RelayEventID: "relay-old", Resolution: ResolutionClean}
	local := &TaskSnapshot{ID: "task-1", Title: "local title", Status: "open", Updated: "2026-01-02T00:00:00Z"}
	relayIssue := issue("task-1", "relay title", "relay-new", time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC))
	merged, err := MergeTaskStateWithOptions(&beadspb.Export{Issues: []*beadspb.Issue{relayIssue}}, []*TaskSnapshot{local}, []*CacheRecord{prev}, MergeOptions{RelayWinsOnConflict: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.Conflicts) != 1 {
		t.Fatalf("conflicts=%d want 1", len(merged.Conflicts))
	}
	if got := merged.Records[0].Resolved.Title; got != "relay title" {
		t.Fatalf("relay should win conflict, got %q", got)
	}
	if got := merged.Export.Issues[0].Title; got != "relay title" {
		t.Fatalf("rendered export should use relay, got %q", got)
	}
}

func TestMigrationLoadsBeadsFixtureAndBuildsRoundTrippableCanonicalEvents(t *testing.T) {
	dir := t.TempDir()
	beadsDir := filepath.Join(dir, ".beads")
	if err := os.MkdirAll(beadsDir, 0755); err != nil {
		t.Fatal(err)
	}
	issues := `{"id":"task-1","title":"Seed task","description":"from beads","status":"open","priority":"p1","epic":"epic-1","labels":["fabric"],"metadata":{"nostr.id":"root-event","nip34.repo_addr":"30617:pub:repo"},"updated":"2026-01-02T00:00:00Z"}
`
	epics := `{"id":"epic-1","name":"Seed epic","status":"open"}
`
	if err := os.WriteFile(filepath.Join(beadsDir, "issues.jsonl"), []byte(issues), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(beadsDir, "epics.jsonl"), []byte(epics), 0644); err != nil {
		t.Fatal(err)
	}
	result, err := Migrate(context.Background(), MigrateOptions{OutDir: dir, CanonicalAuthor: testPubKey(1).Hex(), DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Export.Issues) != 1 || len(result.Export.Epics) != 1 {
		t.Fatalf("export issues=%d epics=%d", len(result.Export.Issues), len(result.Export.Epics))
	}
	var state *gonostr.Event
	for _, ev := range result.Events {
		if ev.Kind == nip34.KindCanonicalState {
			state = ev
		}
	}
	if state == nil {
		t.Fatal("missing canonical task state event")
	}
	state.PubKey = testPubKey(1)
	got, err := nip34.ParseTaskStateEvent(state)
	if err != nil {
		t.Fatal(err)
	}
	if got.Id != "task-1" || got.Title != "Seed task" || got.Priority != beadspb.Priority_PRIORITY_P1 {
		t.Fatalf("round trip mismatch: %#v", got)
	}
}

func TestMigrationPublishUsesCanonicalEvents(t *testing.T) {
	dir := t.TempDir()
	beadsDir := filepath.Join(dir, ".beads")
	if err := os.MkdirAll(beadsDir, 0755); err != nil {
		t.Fatal(err)
	}
	issue := &beadspb.Issue{Id: "task-2", Title: "Publish me", Status: beadspb.Status_STATUS_OPEN, Updated: timestamppb.New(time.Unix(10, 0))}
	ev, err := nip34.BuildTaskStateEvent(issue, testPubKey(1).Hex(), time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := nip34.ParseTaskStateEvent(ev)
	if err != nil || parsed.Id != "task-2" {
		t.Fatalf("fixture sanity parse failed: %#v err=%v", parsed, err)
	}
	if err := os.WriteFile(filepath.Join(beadsDir, "issues.jsonl"), []byte(`{"id":"task-2","title":"Publish me","status":"open"}
`), 0644); err != nil {
		t.Fatal(err)
	}
	pub := &capturePublisher{}
	result, err := Migrate(context.Background(), MigrateOptions{OutDir: dir, CanonicalAuthor: testPubKey(1).Hex(), Relays: []string{"wss://relay.example"}, Signer: noopSigner{}, Publisher: pub})
	if err != nil {
		t.Fatal(err)
	}
	if result.PublishedCount != 1 || len(pub.events) != 1 {
		t.Fatalf("published=%d events=%d", result.PublishedCount, len(pub.events))
	}
}
