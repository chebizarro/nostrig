package durable

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckWritableProbesStateDirectoryWithoutChangingDocuments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "commands.json")
	if err := CheckWritable(path, filepath.Join(filepath.Dir(path), "outbox.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("write probe created durable document: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".nostrig-write-probe-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("write probe leaked temporary files: matches=%v err=%v", matches, err)
	}
	if err := CheckWritable(""); err == nil {
		t.Fatal("expected empty durable path to fail")
	}
}
