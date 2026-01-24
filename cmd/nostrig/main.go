package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/chebizarro/nostrig/internal/converter"
	"github.com/nbd-wtf/go-nostr/nip19"
	"github.com/spf13/cobra"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "nostrig",
		Short: "Fetch NIP-34 git-related Nostr events and render beads-compatible JSONL",
	}

	cmd.AddCommand(newFetchCmd())
	return cmd
}

func newFetchCmd() *cobra.Command {
	var repoID string
	var owner string
	var relays []string
	var outDir string
	var idFormat string
	var idPrefix string

	fetchCmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch a single repo's NIP-34 events and write .beads JSONL artifacts",
		RunE: func(cmd *cobra.Command, args []string) error {
			if repoID == "" {
				return fmt.Errorf("--repo-id is required")
			}
			if outDir == "" {
				return fmt.Errorf("--out is required")
			}

			format, err := converter.ParseIDFormat(idFormat)
			if err != nil {
				return err
			}

			// Convert npub to hex if needed
			ownerHex, err := resolveOwner(owner)
			if err != nil {
				return fmt.Errorf("invalid owner: %w", err)
			}

			opts := converter.FetchOptions{
				RepoID:   repoID,
				Owner:    ownerHex,
				Relays:   relays,
				OutDir:   outDir,
				IDFormat: format,
				IDPrefix: idPrefix,
			}

			p := converter.NewPipeline()
			return p.Run(context.Background(), opts)
		},
	}

	fetchCmd.Flags().StringVar(&repoID, "repo-id", "", "Repository id (d tag) to fetch (required)")
	fetchCmd.Flags().StringVar(&owner, "owner", "", "Repository owner pubkey (hex or npub). Recommended to disambiguate.")
	fetchCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to use (repeatable)")
	fetchCmd.Flags().StringVar(&outDir, "out", ".", "Output directory (writes <out>/.beads/*.jsonl)")
	fetchCmd.Flags().StringVar(&idFormat, "id-format", "spec", "ID format for beads ids: legacy|spec")
	fetchCmd.Flags().StringVar(&idPrefix, "id-prefix", "", "Identifier prefix override for spec format (optional)")

	return fetchCmd
}

// resolveOwner converts an npub to hex, or returns hex as-is.
func resolveOwner(owner string) (string, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return "", nil
	}

	// If it starts with npub, decode it
	if strings.HasPrefix(owner, "npub1") {
		prefix, value, err := nip19.Decode(owner)
		if err != nil {
			return "", fmt.Errorf("failed to decode npub: %w", err)
		}
		if prefix != "npub" {
			return "", fmt.Errorf("expected npub, got %s", prefix)
		}
		hex, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("decoded value is not a string")
		}
		return hex, nil
	}

	// Assume it's already hex
	return owner, nil
}
