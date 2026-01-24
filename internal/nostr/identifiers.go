package nostr

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

const (
	beadsSuffixLen      = 8
	beadsPrefixMaxTotal = 8 // including trailing '-'
)

// NormalizeBeadsPrefix normalizes a raw prefix into a beads identifier prefix.
//
// Rules:
// - lowercases
// - keeps only [a-z0-9-]
// - ensures first rune is a letter (prepending "r" if needed)
// - trims leading/trailing hyphens
// - enforces max length so that after appending trailing hyphen total length ≤ 8
// - ensures it ends with a single hyphen
//
// Returns "" if normalization fails.
func NormalizeBeadsPrefix(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	raw = strings.ToLower(raw)

	var b strings.Builder
	b.Grow(len(raw))

	lastWasDash := false
	for _, r := range raw {
		isAllowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-'
		if !isAllowed {
			continue
		}
		if r == '-' {
			if lastWasDash {
				continue
			}
			lastWasDash = true
			b.WriteRune('-')
			continue
		}
		lastWasDash = false
		b.WriteRune(r)
	}

	core := strings.Trim(b.String(), "-")
	if core == "" {
		return ""
	}

	if core[0] < 'a' || core[0] > 'z' {
		core = "r" + core
		core = strings.Trim(core, "-")
		if core == "" {
			return ""
		}
	}

	maxCoreLen := beadsPrefixMaxTotal - 1 // reserve space for trailing '-'
	if maxCoreLen <= 0 {
		return ""
	}
	if len(core) > maxCoreLen {
		core = core[:maxCoreLen]
		core = strings.Trim(core, "-")
		if core == "" {
			return ""
		}
	}

	if core[0] < 'a' || core[0] > 'z' {
		// In case truncation exposed a non-letter at the front (unlikely, but safe).
		core = "r" + core
		if len(core) > maxCoreLen {
			core = core[:maxCoreLen]
		}
		core = strings.Trim(core, "-")
		if core == "" {
			return ""
		}
	}

	return core + "-"
}

// DefaultBeadsPrefix derives a reasonable default prefix from repo context.
// - candidate from SanitizeSlug(repoID)
// - if empty, uses first 8 chars of owner hex
// - passes through NormalizeBeadsPrefix
// - falls back to "repo-" if still empty
func DefaultBeadsPrefix(repoID, owner string) string {
	candidate := SanitizeSlug(repoID)

	if strings.TrimSpace(candidate) == "" {
		owner = strings.ToLower(strings.TrimSpace(owner))
		if owner != "" {
			if len(owner) > beadsPrefixMaxTotal {
				owner = owner[:beadsPrefixMaxTotal]
			}
			candidate = owner
		}
	}

	prefix := NormalizeBeadsPrefix(candidate)
	if prefix == "" {
		return "repo-"
	}
	return prefix
}

func validateBeadsPrefix(prefix string) (string, error) {
	norm := NormalizeBeadsPrefix(prefix)
	if norm == "" {
		return "", fmt.Errorf("invalid prefix %q", prefix)
	}
	return norm, nil
}

func beadsHashSuffix(input string) string {
	sum := sha256.Sum256([]byte(input))

	// 36^8 fits within uint64: 2,821,109,907,456
	const mod uint64 = 2821109907456

	v := binary.BigEndian.Uint64(sum[:8])
	v = v % mod

	s := strings.ToLower(strconv.FormatUint(v, 36))
	if len(s) < beadsSuffixLen {
		s = strings.Repeat("0", beadsSuffixLen-len(s)) + s
	}

	// Ensure at least one digit to satisfy hash-like heuristics.
	if !hasDigit(s) {
		d := byte('0' + (sum[8] % 10))
		s = s[:beadsSuffixLen-1] + string(d)
	}

	return s
}

func hasDigit(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			return true
		}
	}
	return false
}

// BeadsEpicID returns a spec-compliant epic ID: "<prefix><suffix>" where prefix ends with "-"
// and suffix is a short base36 hash of the repo address.
func BeadsEpicID(prefix, repoOwnerPubKey, repoID string) (string, error) {
	norm, err := validateBeadsPrefix(prefix)
	if err != nil {
		return "", fmt.Errorf("beads epic id: %w", err)
	}

	repoOwnerPubKey = strings.TrimSpace(repoOwnerPubKey)
	repoID = strings.TrimSpace(repoID)
	if repoOwnerPubKey == "" {
		return "", fmt.Errorf("beads epic id: repo owner pubkey is required")
	}
	if repoID == "" {
		return "", fmt.Errorf("beads epic id: repo id is required")
	}

	repoAddr := RepoAddress(repoOwnerPubKey, repoID)
	suffix := beadsHashSuffix(repoAddr)
	return norm + suffix, nil
}

// BeadsIssueID returns a spec-compliant issue ID: "<prefix><suffix>" where prefix ends with "-"
// and suffix is a short base36 hash of "<repoAddr>:<eventID>".
func BeadsIssueID(prefix, repoAddr, eventID string) (string, error) {
	norm, err := validateBeadsPrefix(prefix)
	if err != nil {
		return "", fmt.Errorf("beads issue id: %w", err)
	}

	repoAddr = strings.TrimSpace(repoAddr)
	eventID = strings.TrimSpace(eventID)
	if repoAddr == "" {
		return "", fmt.Errorf("beads issue id: repo address is required")
	}
	if eventID == "" {
		return "", fmt.Errorf("beads issue id: event id is required")
	}

	suffix := beadsHashSuffix(repoAddr + ":" + eventID)
	return norm + suffix, nil
}
