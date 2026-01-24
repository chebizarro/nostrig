package converter

import (
	"fmt"
	"strings"
)

// IDFormat controls how beads IDs are generated.
type IDFormat string

const (
	IDFormatLegacy IDFormat = "legacy"
	IDFormatSpec   IDFormat = "spec"
)

// ParseIDFormat parses an ID format string.
// It normalizes to lowercase, accepts "" as IDFormatSpec, and errors on unknown values.
func ParseIDFormat(s string) (IDFormat, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return IDFormatSpec, nil
	}

	switch IDFormat(s) {
	case IDFormatLegacy, IDFormatSpec:
		return IDFormat(s), nil
	default:
		return "", fmt.Errorf("unknown id format %q (allowed: %s, %s)", s, IDFormatLegacy, IDFormatSpec)
	}
}

func (f IDFormat) String() string {
	return string(f)
}

func (f IDFormat) IsSpec() bool {
	return strings.ToLower(string(f)) == string(IDFormatSpec)
}

func (f IDFormat) IsLegacy() bool {
	return strings.ToLower(string(f)) == string(IDFormatLegacy)
}
