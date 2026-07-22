package main

import (
	"testing"

	"github.com/chebizarro/nostrig/internal/reconcile"
)

func TestNIP34ReconcileCommandRegistered(t *testing.T) {
	command, _, err := newRootCmd().Find([]string{"nip34", "reconcile"})
	if err != nil {
		t.Fatal(err)
	}
	if command == nil || command.Name() != "reconcile" {
		t.Fatalf("command = %#v", command)
	}
	for _, name := range []string{"repo-addr", "author", "relay", "gitea-url", "gitea-repo", "link", "repair"} {
		if command.Flags().Lookup(name) == nil {
			t.Fatalf("missing --%s", name)
		}
	}
}

func TestParseNIP34ExplicitLink(t *testing.T) {
	taskID, number, err := reconcile.ParseLinkSpec("task-1=42")
	if err != nil || taskID != "task-1" || number != 42 {
		t.Fatalf("ParseLinkSpec = %q,%d,%v", taskID, number, err)
	}
	if _, _, err := reconcile.ParseLinkSpec("task-1=0"); err == nil {
		t.Fatal("expected positive issue number validation")
	}
}
