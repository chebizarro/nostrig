// Package durable provides small, reusable primitives for versioned local state.
// The outbox uses it today; the command journal can share the same state directory
// and atomic persistence rules without coupling journal and relay-delivery schemas.
package durable

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// JSONFile atomically persists a JSON document at Path.
type JSONFile[T any] struct {
	Path string
	New  func() T
}

// Load returns a new empty document when the file does not exist.
func (f JSONFile[T]) Load() (T, error) {
	var zero T
	data, err := os.ReadFile(f.Path)
	if os.IsNotExist(err) {
		if f.New != nil {
			return f.New(), nil
		}
		return zero, nil
	}
	if err != nil {
		return zero, fmt.Errorf("read durable state %s: %w", f.Path, err)
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return zero, fmt.Errorf("decode durable state %s: %w", f.Path, err)
	}
	return value, nil
}

// Store writes, fsyncs, and atomically renames a complete JSON document.
func (f JSONFile[T]) Store(value T) error {
	if f.Path == "" {
		return fmt.Errorf("durable state path is required")
	}
	dir := filepath.Dir(f.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create durable state directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".nostrig-state-*")
	if err != nil {
		return fmt.Errorf("create durable state temp file: %w", err)
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod durable state temp file: %w", err)
	}
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(value); err != nil {
		return fmt.Errorf("encode durable state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync durable state temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close durable state temp file: %w", err)
	}
	if err := os.Rename(tmpName, f.Path); err != nil {
		return fmt.Errorf("replace durable state: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open durable state directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync durable state directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close durable state directory: %w", err)
	}
	ok = true
	return nil
}
