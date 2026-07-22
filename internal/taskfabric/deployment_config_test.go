package taskfabric

import (
	"path/filepath"
	"testing"
)

func TestExampleDeploymentACLIsValid(t *testing.T) {
	path := filepath.Join("..", "..", "config", "nostrig-acl.example.json")
	if _, err := LoadAuthorizationConfig(path); err != nil {
		t.Fatalf("load example deployment ACL: %v", err)
	}
}
