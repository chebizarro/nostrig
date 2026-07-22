package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestTaskFlowImportDryRunReportsMigration(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "tasks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "PROJECTS.md"), []byte("| Project | Status |\n| --- | --- |\n| Fleet | active |\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "tasks", "fleet-tasks.md"), []byte("| Task | Status | Priority | Notes |\n| --- | --- | --- | --- |\n| Migrate | open | P9 | preserve me |\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reportPath := filepath.Join(root, "report.json")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"import", "taskflow", "--source", root,
		"--canonical-author", fmt.Sprintf("%064x", 1),
		"--dry-run", "--report", reportPath,
	})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	var report map[string]any
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode report %q: %v", out.String(), err)
	}
	if report["mode"] != "dry-run" || report["tasks_read"] != float64(1) || report["events_generated"] != float64(2) {
		t.Fatalf("report: %#v", report)
	}
	if _, err := os.Stat(reportPath); err != nil {
		t.Fatalf("report file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".nostrig", "taskflow-import-state.json")); !os.IsNotExist(err) {
		t.Fatalf("dry-run wrote state: %v", err)
	}
}
