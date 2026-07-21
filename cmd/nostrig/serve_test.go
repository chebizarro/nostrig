package main

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServeRequiresExplicitStoreAndBackendArguments(t *testing.T) {
	tests := []struct {
		args []string
		want string
	}{
		{[]string{"--relay", "wss://relay.example", "--signet-bunker", "bunker://example"}, "--store must be explicitly set"},
		{[]string{"--relay", "wss://relay.example", "--signet-bunker", "bunker://example", "--store", "bd"}, "--beads-dir is required"},
		{[]string{"--relay", "wss://relay.example", "--signet-bunker", "bunker://example", "--store", "json"}, "--state is required"},
	}
	for _, test := range tests {
		cmd := newServeCmd()
		cmd.SetArgs(test.args)
		if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("args %v error=%v, want %q", test.args, err, test.want)
		}
	}
}

func TestBunkerConnectSecretComesFromProtectedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connect-secret")
	if err := os.WriteFile(path, []byte("authorization-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := bunkerURLWithSecretFile("bunker://0123456789abcdef?relay=wss%3A%2F%2Frelay.example", path)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(got)
	if parsed.Query().Get("secret") != "authorization-token" || parsed.Query().Get("relay") != "wss://relay.example" {
		t.Fatal("assembled bunker query is incorrect")
	}
	if _, err := bunkerURLWithSecretFile("bunker://0123?secret=process-list-leak", ""); err == nil {
		t.Fatal("expected inline secret rejection")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := bunkerURLWithSecretFile("bunker://0123", path); err == nil {
		t.Fatal("expected insecure permission rejection")
	}
}
