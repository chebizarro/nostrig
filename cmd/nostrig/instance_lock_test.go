package main

import (
	"strings"
	"testing"
)

func TestInstanceLockExcludesSecondHolder(t *testing.T) {
	path := t.TempDir() + "/instance.lock"
	first, err := acquireInstanceLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	if _, err := acquireInstanceLock(path); err == nil || !strings.Contains(err.Error(), "another nostrig instance") {
		t.Fatalf("second lock error = %v, want active-instance rejection", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := acquireInstanceLock(path)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	defer second.Close()
}
