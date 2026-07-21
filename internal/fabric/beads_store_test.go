package fabric

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
)

func TestBeadsStoreExportImportRoundTripPreservesUnknownFields(t *testing.T) {
	dir := t.TempDir()
	exportPath := filepath.Join(dir, "export.jsonl")
	importPath := filepath.Join(dir, "import.jsonl")
	script := filepath.Join(dir, "bd")
	records := "" +
		`{"_type":"issue","id":"fp-50","title":"Fabric","description":"full","issue_type":"task","status":"open","priority":0,"assignee":"netward","owner":"biz","labels":["nostr"],"dependencies":[{"issue_id":"fp-50","depends_on_id":"fp-2","type":"blocks"},{"issue_id":"fp-50","depends_on_id":"fp-tf","type":"parent-child"}],"comments":[{"text":"retain me"}],"created_at":"2026-07-20T00:00:00Z","updated_at":"2026-07-20T01:00:00Z"}` + "\n" +
		`{"_type":"issue","id":"fp-tf","title":"Task fabric","description":"epic","issue_type":"epic","status":"in_progress","priority":1,"owner":"biz","custom_future":{"enabled":true},"created_at":"2026-07-19T00:00:00Z","updated_at":"2026-07-20T01:00:00Z"}` + "\n"
	if err := os.WriteFile(exportPath, []byte(records), 0600); err != nil {
		t.Fatal(err)
	}
	program := "#!/bin/sh\ncase \"$*\" in\n  *export*) cat \"$FAKE_BD_EXPORT\" ;;\n  *import*) cat > \"$FAKE_BD_IMPORT\"; printf '{}\\n' ;;\n  *) exit 2 ;;\nesac\n"
	if err := os.WriteFile(script, []byte(program), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_BD_EXPORT", exportPath)
	t.Setenv("FAKE_BD_IMPORT", importPath)
	store := &BeadsStore{Directory: dir, Binary: script, Actor: "nostrig-test"}
	got, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Issues) != 1 || len(got.Epics) != 1 {
		t.Fatalf("issues=%d epics=%d", len(got.Issues), len(got.Epics))
	}
	if got.Issues[0].Priority != beadspb.Priority_PRIORITY_P0 || len(got.Issues[0].DependsOn) != 1 || got.Issues[0].Epic != "fp-tf" {
		t.Fatalf("mapping failed: %v", got.Issues[0])
	}
	got.Issues[0].Status = beadspb.Status_STATUS_IN_PROGRESS
	if err := store.Save(context.Background(), got); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(importPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	rows := []map[string]any{}
	for scanner.Scan() {
		var row map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	if len(rows) != 2 {
		t.Fatalf("import rows=%d", len(rows))
	}
	byID := make(map[string]map[string]any, len(rows))
	for _, row := range rows {
		byID[row["id"].(string)] = row
	}
	if byID["fp-50"]["owner"] != "biz" || byID["fp-50"]["comments"] == nil || byID["fp-50"]["status"] != "in_progress" {
		t.Fatalf("issue lost fields: %#v", byID["fp-50"])
	}
	if byID["fp-tf"]["custom_future"] == nil || byID["fp-tf"]["issue_type"] != "epic" {
		t.Fatalf("epic lost fields: %#v", byID["fp-tf"])
	}
}

func TestBeadsStoreRequiresExplicitWorkspace(t *testing.T) {
	store := new(BeadsStore)
	if _, err := store.Load(context.Background()); err == nil {
		t.Fatal("expected workspace validation error")
	}
}
