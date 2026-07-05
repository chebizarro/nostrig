package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/chebizarro/nostrig/internal/converter"
	"github.com/chebizarro/nostrig/internal/fabric"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"github.com/chebizarro/nostrig/internal/taskfabric"
	gonostr "github.com/nbd-wtf/go-nostr"
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
		Short: "Relay-backed NIP-34 to beads task-fabric bridge",
	}

	cmd.AddCommand(newFetchCmd(), newServeCmd(), newPublishCmd(), newSyncCmd(), newClaimCmd())
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
			format, ownerHex, err := parseFetchInputs(repoID, owner, idFormat)
			if err != nil {
				return err
			}
			if outDir == "" {
				return fmt.Errorf("--out is required")
			}

			opts := converter.FetchOptions{RepoID: repoID, Owner: ownerHex, Relays: relays, OutDir: outDir, IDFormat: format, IDPrefix: idPrefix}
			return converter.NewPipeline().Run(cmd.Context(), opts)
		},
	}

	addFetchFlags(fetchCmd, &repoID, &owner, &relays, &idFormat, &idPrefix)
	fetchCmd.Flags().StringVar(&outDir, "out", ".", "Output directory (writes <out>/.beads/*.jsonl)")

	return fetchCmd
}

func newPublishCmd() *cobra.Command {
	var repoID string
	var owner string
	var relays []string
	var idFormat string
	var idPrefix string
	var privateKey string
	var dryRun bool

	publishCmd := &cobra.Command{
		Use:   "publish",
		Short: "Publish converted NIP-34 work items as canonical task-fabric events",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			format, ownerHex, err := parseFetchInputs(repoID, owner, idFormat)
			if err != nil {
				return err
			}

			result, err := converter.NewPipeline().Export(ctx, converter.FetchOptions{RepoID: repoID, Owner: ownerHex, Relays: relays, IDFormat: format, IDPrefix: idPrefix})
			if err != nil {
				return err
			}
			events, err := nip34.BuildCanonicalEvents(result.Export, time.Now().UTC())
			if err != nil {
				return err
			}
			if len(events) == 0 {
				return fmt.Errorf("no canonical events generated")
			}

			signer, _, err := signerFromFlags(ctx, privateKey, !dryRun)
			if err != nil {
				return err
			}
			if dryRun {
				if signer != nil {
					if err := signEvents(ctx, signer, events); err != nil {
						return err
					}
				}
				return writeEvents(cmd, events)
			}

			if err := nip34.NewPublisher().Publish(ctx, result.Relays, signer, events); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "published %d event(s) to %d relay(s)\n", len(events), len(result.Relays))
			return err
		},
	}

	addFetchFlags(publishCmd, &repoID, &owner, &relays, &idFormat, &idPrefix)
	addSigningFlags(publishCmd, &privateKey, &dryRun)
	return publishCmd
}

func newSyncCmd() *cobra.Command {
	var repoID string
	var owner string
	var repoAddr string
	var relays []string
	var authors []string
	var taskIDs []string
	var outDir string
	var limit int

	syncCmd := &cobra.Command{
		Use:   "sync",
		Short: "Pull canonical task-state events from relays and render .beads JSONL",
		RunE: func(cmd *cobra.Command, args []string) error {
			ownerHex, err := resolveOwner(owner)
			if err != nil {
				return fmt.Errorf("invalid owner: %w", err)
			}
			addr := strings.TrimSpace(repoAddr)
			if addr == "" && strings.TrimSpace(repoID) != "" && ownerHex != "" {
				addr = nip34.RepoAddress(ownerHex, repoID)
			}
			relays = relaysWithEnv(relays)
			result, err := taskfabric.Sync(cmd.Context(), nip34.NewClient(), taskfabric.SyncOptions{Relays: relays, RepoAddr: addr, TaskIDs: taskIDs, Authors: authors, OutDir: outDir, Limit: limit})
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "synced %d task(s) from %d event(s) into %s/.beads\n", len(result.Export.Issues), result.EventCount, strings.TrimRight(outDir, "/"))
			return err
		},
	}

	syncCmd.Flags().StringVar(&repoID, "repo-id", "", "Repository id (d tag); used with --owner to derive --repo-addr")
	syncCmd.Flags().StringVar(&owner, "owner", "", "Repository owner pubkey (hex or npub); used with --repo-id to derive --repo-addr")
	syncCmd.Flags().StringVar(&repoAddr, "repo-addr", "", "Canonical repo address tag to sync (30617:<owner>:<repo-id>)")
	syncCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to use (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	syncCmd.Flags().StringSliceVar(&authors, "author", nil, "Optional canonical task-state event author pubkey filter (repeatable)")
	syncCmd.Flags().StringSliceVar(&taskIDs, "task-id", nil, "Task id(s) to sync by exact d tag (repeatable)")
	syncCmd.Flags().StringVar(&outDir, "out", ".", "Output directory (writes <out>/.beads/issues.jsonl)")
	syncCmd.Flags().IntVar(&limit, "limit", 500, "Maximum task-state events to request")
	return syncCmd
}

func newClaimCmd() *cobra.Command {
	var taskID string
	var claimer string
	var recipient string
	var relays []string
	var privateKey string
	var dryRun bool

	claimCmd := &cobra.Command{
		Use:   "claim",
		Short: "Publish a ContextVM task/claim command for a worker/agent",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if strings.TrimSpace(taskID) == "" {
				return fmt.Errorf("--task-id is required")
			}
			if strings.TrimSpace(recipient) == "" {
				return fmt.Errorf("--recipient is required")
			}

			key, err := resolvePrivateKey(privateKey)
			if err != nil {
				return err
			}
			if strings.TrimSpace(claimer) == "" && key != "" {
				claimer, err = gonostr.GetPublicKey(key)
				if err != nil {
					return fmt.Errorf("derive claimer pubkey: %w", err)
				}
			}
			if strings.TrimSpace(claimer) == "" {
				return fmt.Errorf("--claimer is required when no private key is available to derive it")
			}

			event, err := nip34.BuildClaimDispatch(taskID, claimer, recipient, time.Now().UTC())
			if err != nil {
				return err
			}
			events := []*gonostr.Event{event}
			signer, _, err := signerFromResolvedKey(ctx, key, !dryRun)
			if err != nil {
				return err
			}
			if dryRun {
				if signer != nil {
					if err := signEvents(ctx, signer, events); err != nil {
						return err
					}
				}
				return writeEvents(cmd, events)
			}

			relays = relaysWithEnv(relays)
			if len(relays) == 0 {
				return fmt.Errorf("at least one --relay or NOSTR_RELAY is required")
			}
			if err := nip34.NewPublisher().Publish(ctx, relays, signer, events); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "published claim for %s to %d relay(s)\n", taskID, len(relays))
			return err
		},
	}

	claimCmd.Flags().StringVar(&taskID, "task-id", "", "Task id to claim (required)")
	claimCmd.Flags().StringVar(&claimer, "claimer", "", "Claiming agent/worker pubkey or stable id; defaults to signer pubkey when --private-key is available")
	claimCmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	claimCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(claimCmd, &privateKey, &dryRun)
	return claimCmd
}

func addFetchFlags(cmd *cobra.Command, repoID *string, owner *string, relays *[]string, idFormat *string, idPrefix *string) {
	cmd.Flags().StringVar(repoID, "repo-id", "", "Repository id (d tag) to fetch (required)")
	cmd.Flags().StringVar(owner, "owner", "", "Repository owner pubkey (hex or npub). Recommended to disambiguate.")
	cmd.Flags().StringSliceVar(relays, "relay", nil, "Relay websocket URL(s) to use (repeatable)")
	cmd.Flags().StringVar(idFormat, "id-format", "spec", "ID format for beads ids: legacy|spec")
	cmd.Flags().StringVar(idPrefix, "id-prefix", "", "Identifier prefix override for spec format (optional)")
}

func addSigningFlags(cmd *cobra.Command, privateKey *string, dryRun *bool) {
	cmd.Flags().StringVar(privateKey, "private-key", "", "Local-dev Nostr private key (hex or nsec); defaults to NOSTR_PRIVATE_KEY")
	cmd.Flags().BoolVar(dryRun, "dry-run", false, "Print generated events as JSONL instead of publishing")
}

func parseFetchInputs(repoID, owner, idFormat string) (converter.IDFormat, string, error) {
	if strings.TrimSpace(repoID) == "" {
		return "", "", fmt.Errorf("--repo-id is required")
	}
	format, err := converter.ParseIDFormat(idFormat)
	if err != nil {
		return "", "", err
	}
	ownerHex, err := resolveOwner(owner)
	if err != nil {
		return "", "", fmt.Errorf("invalid owner: %w", err)
	}
	return format, ownerHex, nil
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

func resolvePrivateKey(privateKey string) (string, error) {
	key := strings.TrimSpace(privateKey)
	if key == "" {
		key = strings.TrimSpace(os.Getenv("NOSTR_PRIVATE_KEY"))
	}
	if key == "" {
		return "", nil
	}
	if strings.HasPrefix(key, "nsec1") {
		prefix, value, err := nip19.Decode(key)
		if err != nil {
			return "", fmt.Errorf("failed to decode nsec: %w", err)
		}
		if prefix != "nsec" {
			return "", fmt.Errorf("expected nsec, got %s", prefix)
		}
		hex, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("decoded nsec value is not a string")
		}
		return hex, nil
	}
	return key, nil
}

func signerFromFlags(ctx context.Context, privateKey string, required bool) (nip34.Signer, bool, error) {
	key, err := resolvePrivateKey(privateKey)
	if err != nil {
		return nil, false, err
	}
	return signerFromResolvedKey(ctx, key, required)
}

func signerFromResolvedKey(ctx context.Context, key string, required bool) (nip34.Signer, bool, error) {
	if strings.TrimSpace(key) == "" {
		if required {
			return nil, false, fmt.Errorf("signing requires --private-key or NOSTR_PRIVATE_KEY for this MVP; production deployments should use Signet/NIP-46")
		}
		return nil, false, nil
	}
	signer, err := nip34.NewPrivateKeySigner(key)
	if err != nil {
		return nil, false, err
	}
	return signer, true, ctx.Err()
}

func signEvents(ctx context.Context, signer nip34.Signer, events []*gonostr.Event) error {
	for _, ev := range events {
		if ev == nil {
			continue
		}
		if err := signer.SignEvent(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

func writeEvents(cmd *cobra.Command, events []*gonostr.Event) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	for _, ev := range events {
		if ev == nil {
			continue
		}
		if err := enc.Encode(ev); err != nil {
			return err
		}
	}
	return nil
}

func relaysWithEnv(relays []string) []string {
	out := make([]string, 0, len(relays)+2)
	out = append(out, relays...)
	if len(cleanStrings(out)) == 0 {
		out = append(out, splitEnvList(os.Getenv("NOSTR_RELAYS"))...)
		out = append(out, splitEnvList(os.Getenv("NOSTR_RELAY"))...)
	}
	return cleanStrings(out)
}

func splitEnvList(v string) []string {
	return strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\t' })
}

func cleanStrings(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
