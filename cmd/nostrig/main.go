package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/chebizarro/nostrig/internal/converter"
	"github.com/chebizarro/nostrig/internal/fabric"
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
	cmd.AddCommand(newServeCmd())
	return cmd
}

func newServeCmd() *cobra.Command {
	var relays []string
	var bunkerURL, connectSecretFile, storeKind, beadsDir, statePath, bdBinary, actor string
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the bidirectional bd/relay task-fabric adapter",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if len(relays) == 0 {
				return fmt.Errorf("at least one --relay is required")
			}
			if bunkerURL == "" {
				return fmt.Errorf("--signet-bunker is required (raw keys are not supported)")
			}
			var store fabric.Store
			switch storeKind {
			case "bd":
				if beadsDir == "" {
					return fmt.Errorf("--beads-dir is required with --store=bd")
				}
				store = &fabric.BeadsStore{Directory: beadsDir, Binary: bdBinary, Actor: actor}
			case "json":
				if statePath == "" {
					return fmt.Errorf("--state is required with --store=json")
				}
				store = &fabric.JSONStore{Path: statePath}
			default:
				return fmt.Errorf("--store must be explicitly set to bd or json")
			}
			preparedBunkerURL, err := bunkerURLWithSecretFile(bunkerURL, connectSecretFile)
			if err != nil {
				return err
			}
			signer, err := fabric.NewBunkerSigner(cmd.Context(), preparedBunkerURL)
			if err != nil {
				return err
			}
			pubkey, err := signer.PublicKey(cmd.Context())
			if err != nil {
				return err
			}
			relayPublishers := make([]fabric.Relay, 0, len(relays))
			for _, url := range relays {
				relayPublishers = append(relayPublishers, fabric.WebsocketRelay{URL: url})
			}
			service := &fabric.Service{
				Store:     store,
				Source:    &fabric.RelaySource{Relays: relays},
				Publisher: &fabric.Publisher{Signer: signer, Relays: relayPublishers},
				PubKey:    pubkey, Interval: interval,
			}
			return service.Run(cmd.Context())
		},
	}
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s)")
	cmd.Flags().StringVar(&bunkerURL, "signet-bunker", "", "Signet NIP-46 bunker URL")
	cmd.Flags().StringVar(&connectSecretFile, "signet-connect-secret-file", "", "0600 file containing the NIP-46 connect secret (never pass it in the URL)")
	cmd.Flags().StringVar(&storeKind, "store", "", "Required ledger backend: bd (production) or json (development/test)")
	cmd.Flags().StringVar(&beadsDir, "beads-dir", "", "Beads workspace used by bd export/import")
	cmd.Flags().StringVar(&bdBinary, "bd-bin", "bd", "bd executable")
	cmd.Flags().StringVar(&actor, "actor", "nostrig", "Beads audit actor")
	cmd.Flags().StringVar(&statePath, "state", "", "Development JSON-store path (only with --store=json)")
	cmd.Flags().DurationVar(&interval, "interval", 15*time.Second, "Relay reconciliation interval")
	return cmd
}

func bunkerURLWithSecretFile(input, secretFile string) (string, error) {
	parsed, err := url.Parse(input)
	if err != nil || parsed.Scheme != "bunker" {
		return "", fmt.Errorf("invalid Signet bunker URL")
	}
	query := parsed.Query()
	if query.Has("secret") {
		return "", fmt.Errorf("inline NIP-46 secret is forbidden; use --signet-connect-secret-file")
	}
	if secretFile == "" {
		return input, nil
	}
	info, err := os.Stat(secretFile)
	if err != nil {
		return "", fmt.Errorf("read NIP-46 connect secret file: %w", err)
	}
	if info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("NIP-46 connect secret file must not be accessible by group or others")
	}
	secret, err := os.ReadFile(secretFile)
	if err != nil {
		return "", fmt.Errorf("read NIP-46 connect secret file: %w", err)
	}
	value := strings.TrimSpace(string(secret))
	if value == "" {
		return "", fmt.Errorf("NIP-46 connect secret file is empty")
	}
	query.Set("secret", value)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
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
