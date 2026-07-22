package taskflow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

type capturePublisher struct {
	events []*gonostr.Event
}

func (p *capturePublisher) Publish(_ context.Context, _ []string, _ nip34.Signer, events []*gonostr.Event) error {
	p.events = append(p.events, events...)
	return nil
}

type noopSigner struct{}

func (noopSigner) SignEvent(context.Context, *gonostr.Event) error { return nil }

func writeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "tasks"), 0o755); err != nil {
		t.Fatal(err)
	}
	projects := "# Projects\n\n| Project | Status | Priority | Notes |\n| --- | --- | --- | --- |\n| Fleet Planning | Active | P1 | Migration project |\n"
	tasks := "# Fleet Planning Tasks\n\n| Task | Status | Priority | Notes | Checkpoint |\n| --- | --- | --- | --- | --- |\n| Import TaskFlow | In Progress | High | Markdown and SQLite diverged | Parser fixture complete |\n| Retire TaskFlow | Parking Lot | P9 | Read-only after cutover | |\n"
	if err := os.WriteFile(filepath.Join(root, "PROJECTS.md"), []byte(projects), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "tasks", "fleet-planning-tasks.md"), []byte(tasks), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestParseDirectoryMapsStatusPriorityNotesAndCheckpoints(t *testing.T) {
	root := writeFixture(t)
	projects, tasks, warnings, err := ParseDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 || len(projects) != 1 || len(tasks) != 2 {
		t.Fatalf("projects=%d tasks=%d warnings=%v", len(projects), len(tasks), warnings)
	}
	if tasks[0].Status != "in_progress" || tasks[0].Priority != "P1" {
		t.Fatalf("first mapping: %#v", tasks[0])
	}
	if tasks[1].Status != "deferred" || tasks[1].Priority != "P9" {
		t.Fatalf("P9 mapping: %#v", tasks[1])
	}
	doc := taskDocument(tasks[0])
	if len(doc.Comments) != 1 || doc.Comments[0].Text != "Markdown and SQLite diverged" {
		t.Fatalf("comments: %#v", doc.Comments)
	}
	if len(doc.Checkpoints) != 1 || doc.Checkpoints[0].Summary != "Parser fixture complete" {
		t.Fatalf("checkpoints: %#v", doc.Checkpoints)
	}
}

func TestRunIsIdempotentAfterSuccessfulPublish(t *testing.T) {
	root := writeFixture(t)
	statePath := filepath.Join(t.TempDir(), "state.json")
	author := fmt.Sprintf("%064x", 1)
	publisher := &capturePublisher{}
	opts := Options{
		Source: root, StatePath: statePath, CanonicalAuthor: author,
		Relays: []string{"wss://relay.example"}, Signer: noopSigner{},
		Publisher: publisher, Now: time.Unix(100, 0),
	}
	first, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.Published != 3 || first.RecordsSkipped != 0 {
		t.Fatalf("first report: %#v", first)
	}
	second, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.Published != 0 || second.EventsGenerated != 0 || second.RecordsSkipped != 3 {
		t.Fatalf("second report: %#v", second)
	}
	if len(publisher.events) != 3 {
		t.Fatalf("published %d total events, want 3", len(publisher.events))
	}
}

func TestDryRunDoesNotWriteIdempotencyState(t *testing.T) {
	root := writeFixture(t)
	statePath := filepath.Join(t.TempDir(), "state.json")
	report, err := Run(context.Background(), Options{
		Source: root, StatePath: statePath, CanonicalAuthor: fmt.Sprintf("%064x", 1),
		DryRun: true, Now: time.Unix(100, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Mode != "dry-run" || report.EventsGenerated != 3 {
		t.Fatalf("report: %#v", report)
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("dry-run wrote state: %v", err)
	}
}
