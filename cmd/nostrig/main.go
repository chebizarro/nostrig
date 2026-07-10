package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/chebizarro/nostrig/internal/converter"
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

	cmd.AddCommand(newFetchCmd(), newPublishCmd(), newSyncCmd(), newClaimCmd(), newAssignCmd(), newUpdateCmd(), newCloseCmd(), newQueueCmd(), newServeCmd())
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
	var signing signingOptions

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

			signer, _, err := signerFromOptions(ctx, signing, !signing.dryRun)
			if err != nil {
				return err
			}
			if signing.dryRun {
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
	addSigningFlags(publishCmd, &signing)
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
	var cachePath string
	var limit int
	var failOnConflict bool

	syncCmd := &cobra.Command{
		Use:   "sync",
		Short: "Pull canonical task-state events into the durable task cache and render .beads JSONL",
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
			result, err := taskfabric.Sync(cmd.Context(), nip34.NewClient(), taskfabric.SyncOptions{Relays: relays, RepoAddr: addr, TaskIDs: taskIDs, Authors: authors, OutDir: outDir, CachePath: cachePath, Limit: limit, FailOnConflict: failOnConflict})
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "synced %d task(s) from %d event(s) into %s/.beads via cache %s (%d conflict(s))\n", len(result.Export.Issues), result.EventCount, strings.TrimRight(outDir, "/"), result.CachePath, result.ConflictCount)
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
	syncCmd.Flags().StringVar(&cachePath, "cache", "", "Durable nostrig task cache path (default: <out>/.nostrig/task-cache.jsonl)")
	syncCmd.Flags().BoolVar(&failOnConflict, "fail-on-conflict", false, "Exit non-zero when local and relay changes conflict")
	syncCmd.Flags().IntVar(&limit, "limit", 500, "Maximum task-state events to request")
	return syncCmd
}

func newClaimCmd() *cobra.Command {
	var taskID string
	var claimer string
	var recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions

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
			signer, _, err := signerFromOptions(ctx, signing, !signing.dryRun)
			if err != nil {
				return err
			}
			if strings.TrimSpace(claimer) == "" && signer != nil {
				claimer, err = publicKeyFromSigner(ctx, signer)
				if err != nil {
					return err
				}
			}
			if strings.TrimSpace(claimer) == "" {
				return fmt.Errorf("--claimer is required when no signer public key is available")
			}

			event, err := nip34.BuildClaimDispatch(taskID, claimer, recipient, time.Now().UTC())
			if err != nil {
				return err
			}
			events := []*gonostr.Event{event}
			if signing.dryRun {
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
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "published claim for %s to %d relay(s)\n", taskID, len(relays)); err != nil {
				return err
			}
			return maybeWaitForResponse(cmd, relays, event, response)
		},
	}

	claimCmd.Flags().StringVar(&taskID, "task-id", "", "Task id to claim (required)")
	claimCmd.Flags().StringVar(&claimer, "claimer", "", "Claiming agent/worker pubkey or stable id; defaults to signer pubkey when available")
	claimCmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	claimCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(claimCmd, &signing)
	addResponseFlags(claimCmd, &response)
	return claimCmd
}

func newAssignCmd() *cobra.Command {
	var taskID string
	var assignee string
	var recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions

	assignCmd := &cobra.Command{
		Use:   "assign",
		Short: "Publish a ContextVM task/assign command",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if strings.TrimSpace(taskID) == "" {
				return fmt.Errorf("--task-id is required")
			}
			if strings.TrimSpace(assignee) == "" {
				return fmt.Errorf("--assignee is required")
			}
			if strings.TrimSpace(recipient) == "" {
				return fmt.Errorf("--recipient is required")
			}
			signer, _, err := signerFromOptions(ctx, signing, !signing.dryRun)
			if err != nil {
				return err
			}
			event, err := nip34.BuildAssignCommand(taskID, assignee, recipient, time.Now().UTC())
			if err != nil {
				return err
			}
			events := []*gonostr.Event{event}
			if signing.dryRun {
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
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "published assignment for %s to %d relay(s)\n", taskID, len(relays)); err != nil {
				return err
			}
			return maybeWaitForResponse(cmd, relays, event, response)
		},
	}

	assignCmd.Flags().StringVar(&taskID, "task-id", "", "Task id to assign (required)")
	assignCmd.Flags().StringVar(&assignee, "assignee", "", "Assignee agent/worker pubkey or stable id (required)")
	assignCmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	assignCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(assignCmd, &signing)
	addResponseFlags(assignCmd, &response)
	return assignCmd
}

func newUpdateCmd() *cobra.Command {
	var taskID string
	var recipient string
	var status string
	var assignee string
	var title string
	var description string
	var relays []string
	var signing signingOptions
	var response responseOptions

	updateCmd := &cobra.Command{
		Use:   "update",
		Short: "Publish a ContextVM task/update command",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if strings.TrimSpace(taskID) == "" {
				return fmt.Errorf("--task-id is required")
			}
			if strings.TrimSpace(recipient) == "" {
				return fmt.Errorf("--recipient is required")
			}
			params := map[string]any{"task_id": taskID}
			if strings.TrimSpace(status) != "" {
				params["status"] = strings.TrimSpace(status)
			}
			if strings.TrimSpace(assignee) != "" {
				params["assignee"] = strings.TrimSpace(assignee)
			}
			if strings.TrimSpace(title) != "" {
				params["title"] = strings.TrimSpace(title)
			}
			if strings.TrimSpace(description) != "" {
				params["description"] = strings.TrimSpace(description)
			}
			if len(params) == 1 {
				return fmt.Errorf("provide at least one update field: --status, --assignee, --title, or --description")
			}
			signer, _, err := signerFromOptions(ctx, signing, !signing.dryRun)
			if err != nil {
				return err
			}
			event, err := nip34.BuildContextVMCommand("task/update", recipient, params, time.Now().UTC())
			if err != nil {
				return err
			}
			events := []*gonostr.Event{event}
			if signing.dryRun {
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
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "published update for %s to %d relay(s)\n", taskID, len(relays)); err != nil {
				return err
			}
			return maybeWaitForResponse(cmd, relays, event, response)
		},
	}

	updateCmd.Flags().StringVar(&taskID, "task-id", "", "Task id to update (required)")
	updateCmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	updateCmd.Flags().StringVar(&status, "status", "", "New task status")
	updateCmd.Flags().StringVar(&assignee, "assignee", "", "New task assignee")
	updateCmd.Flags().StringVar(&title, "title", "", "New task title")
	updateCmd.Flags().StringVar(&description, "description", "", "New task description")
	updateCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(updateCmd, &signing)
	addResponseFlags(updateCmd, &response)
	return updateCmd
}

func newCloseCmd() *cobra.Command {
	var taskID string
	var recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions

	closeCmd := &cobra.Command{
		Use:   "close",
		Short: "Publish a ContextVM task/close command",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(taskID) == "" {
				return fmt.Errorf("--task-id is required")
			}
			if strings.TrimSpace(recipient) == "" {
				return fmt.Errorf("--recipient is required")
			}
			event, err := nip34.BuildCloseCommand(taskID, recipient, time.Now().UTC())
			if err != nil {
				return err
			}
			return publishContextVMCommand(cmd, event, relays, signing, response, fmt.Sprintf("published close for %s", taskID))
		},
	}
	closeCmd.Flags().StringVar(&taskID, "task-id", "", "Task id to close (required)")
	closeCmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	closeCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(closeCmd, &signing)
	addResponseFlags(closeCmd, &response)
	return closeCmd
}

func newQueueCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "queue", Short: "Publish ContextVM queue commands"}
	cmd.AddCommand(newQueueEnqueueCmd(), newQueueDequeueCmd(), newQueueListCmd())
	return cmd
}

func newQueueEnqueueCmd() *cobra.Command {
	var queue, taskID, recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions
	cmd := &cobra.Command{Use: "enqueue", Short: "Publish a ContextVM queue/enqueue command", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(taskID) == "" {
			return fmt.Errorf("--task-id is required")
		}
		if strings.TrimSpace(recipient) == "" {
			return fmt.Errorf("--recipient is required")
		}
		ev, err := nip34.BuildQueueEnqueueCommand(queue, taskID, recipient, time.Now().UTC())
		if err != nil {
			return err
		}
		return publishContextVMCommand(cmd, ev, relays, signing, response, fmt.Sprintf("published enqueue for %s", taskID))
	}}
	cmd.Flags().StringVar(&queue, "queue", "backlog", "Queue name")
	cmd.Flags().StringVar(&taskID, "task-id", "", "Task id to enqueue (required)")
	cmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(cmd, &signing)
	addResponseFlags(cmd, &response)
	return cmd
}

func newQueueDequeueCmd() *cobra.Command {
	var queue, recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions
	cmd := &cobra.Command{Use: "dequeue", Short: "Publish a ContextVM queue/dequeue command", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(recipient) == "" {
			return fmt.Errorf("--recipient is required")
		}
		ev, err := nip34.BuildQueueDequeueCommand(queue, recipient, time.Now().UTC())
		if err != nil {
			return err
		}
		return publishContextVMCommand(cmd, ev, relays, signing, response, fmt.Sprintf("published dequeue for %s", queue))
	}}
	cmd.Flags().StringVar(&queue, "queue", "backlog", "Queue name")
	cmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(cmd, &signing)
	addResponseFlags(cmd, &response)
	return cmd
}

func newQueueListCmd() *cobra.Command {
	var queue, recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions
	cmd := &cobra.Command{Use: "list", Short: "Publish a ContextVM queue/list command", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(recipient) == "" {
			return fmt.Errorf("--recipient is required")
		}
		ev, err := nip34.BuildQueueListCommand(queue, recipient, time.Now().UTC())
		if err != nil {
			return err
		}
		return publishContextVMCommand(cmd, ev, relays, signing, response, fmt.Sprintf("published list for %s", queue))
	}}
	cmd.Flags().StringVar(&queue, "queue", "backlog", "Queue name")
	cmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(cmd, &signing)
	addResponseFlags(cmd, &response)
	return cmd
}

func newServeCmd() *cobra.Command {
	var relays []string
	var signing signingOptions
	var pubkey string
	cmd := &cobra.Command{Use: "serve", Short: "Serve incoming ContextVM task and queue intents", RunE: func(cmd *cobra.Command, args []string) error {
		signer, _, err := signerFromOptions(cmd.Context(), signing, true)
		if err != nil {
			return err
		}
		return taskfabric.Serve(cmd.Context(), taskfabric.ServeOptions{Relays: relaysWithEnv(relays), Signer: signer, PubKey: pubkey})
	}}
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to subscribe/publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	cmd.Flags().StringVar(&pubkey, "pubkey", "", "Server recipient pubkey; defaults to signer pubkey when available")
	addSigningFlags(cmd, &signing)
	return cmd
}

func publishContextVMCommand(cmd *cobra.Command, event *gonostr.Event, relays []string, signing signingOptions, response responseOptions, msg string) error {
	ctx := cmd.Context()
	events := []*gonostr.Event{event}
	signer, _, err := signerFromOptions(ctx, signing, !signing.dryRun)
	if err != nil {
		return err
	}
	if signing.dryRun {
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
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s to %d relay(s)\n", msg, len(relays)); err != nil {
		return err
	}
	return maybeWaitForResponse(cmd, relays, event, response)
}

func addFetchFlags(cmd *cobra.Command, repoID *string, owner *string, relays *[]string, idFormat *string, idPrefix *string) {
	cmd.Flags().StringVar(repoID, "repo-id", "", "Repository id (d tag) to fetch (required)")
	cmd.Flags().StringVar(owner, "owner", "", "Repository owner pubkey (hex or npub). Recommended to disambiguate.")
	cmd.Flags().StringSliceVar(relays, "relay", nil, "Relay websocket URL(s) to use (repeatable)")
	cmd.Flags().StringVar(idFormat, "id-format", "spec", "ID format for beads ids: legacy|spec")
	cmd.Flags().StringVar(idPrefix, "id-prefix", "", "Identifier prefix override for spec format (optional)")
}

type signingOptions struct {
	privateKey      string
	bunkerURL       string
	clientSecretKey string
	dryRun          bool
}

type responseOptions struct {
	wait    bool
	timeout time.Duration
}

func addSigningFlags(cmd *cobra.Command, opts *signingOptions) {
	cmd.Flags().StringVar(&opts.bunkerURL, "signer-bunker-url", "", "Signet/NIP-46 bunker URL; defaults to NOSTRIG_SIGNER_BUNKER_URL")
	cmd.Flags().StringVar(&opts.clientSecretKey, "signer-client-secret-key", "", "NIP-46 client secret key (hex); defaults to NOSTRIG_SIGNER_CLIENT_SECRET_KEY and may be ephemeral if omitted")
	cmd.Flags().StringVar(&opts.privateKey, "private-key", "", "Local-dev Nostr private key (hex); defaults to NOSTR_PRIVATE_KEY and is forbidden when NOSTRIG_ENV=production")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Print generated events as JSONL instead of publishing")
}

func addResponseFlags(cmd *cobra.Command, opts *responseOptions) {
	cmd.Flags().BoolVar(&opts.wait, "wait-response", false, "Wait for a correlated ContextVM JSON-RPC response after publishing")
	cmd.Flags().DurationVar(&opts.timeout, "response-timeout", 30*time.Second, "Maximum time to wait for --wait-response")
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

	return owner, nil
}

func resolvePrivateKey(privateKey string) (string, error) {
	key := strings.TrimSpace(privateKey)
	if key == "" {
		key = strings.TrimSpace(os.Getenv("NOSTR_PRIVATE_KEY"))
	}
	return normalizeSecretKey(key)
}

func resolveClientSecretKey(clientSecretKey string) (string, error) {
	key := strings.TrimSpace(clientSecretKey)
	if key == "" {
		key = strings.TrimSpace(os.Getenv("NOSTRIG_SIGNER_CLIENT_SECRET_KEY"))
	}
	if key == "" {
		key = strings.TrimSpace(os.Getenv("NOSTRIG_SIGNER_SECRET_KEY"))
	}
	return normalizeSecretKey(key)
}

func normalizeSecretKey(key string) (string, error) {
	return strings.TrimSpace(key), nil
}

func signerFromOptions(ctx context.Context, opts signingOptions, required bool) (nip34.Signer, bool, error) {
	bunkerURL := strings.TrimSpace(opts.bunkerURL)
	if bunkerURL == "" {
		bunkerURL = strings.TrimSpace(os.Getenv("NOSTRIG_SIGNER_BUNKER_URL"))
	}
	key, err := resolvePrivateKey(opts.privateKey)
	if err != nil {
		return nil, false, err
	}
	production := isProductionEnv()
	if bunkerURL != "" && key != "" {
		return nil, false, fmt.Errorf("configure either Signet/NIP-46 (--signer-bunker-url) or local-dev --private-key, not both")
	}
	if key != "" {
		if production {
			return nil, false, fmt.Errorf("raw --private-key/NOSTR_PRIVATE_KEY is forbidden when NOSTRIG_ENV=production; use --signer-bunker-url")
		}
		signer, err := nip34.NewPrivateKeySigner(key)
		if err != nil {
			return nil, false, err
		}
		return signer, true, ctx.Err()
	}
	if bunkerURL != "" {
		clientSecretKey, err := resolveClientSecretKey(opts.clientSecretKey)
		if err != nil {
			return nil, false, err
		}
		signer, err := nip34.ConnectNIP46Signer(ctx, bunkerURL, clientSecretKey)
		if err != nil {
			return nil, false, err
		}
		return signer, true, ctx.Err()
	}
	if required {
		if production {
			return nil, false, fmt.Errorf("production signing requires --signer-bunker-url or NOSTRIG_SIGNER_BUNKER_URL")
		}
		return nil, false, fmt.Errorf("signing requires --signer-bunker-url/NOSTRIG_SIGNER_BUNKER_URL or local-dev --private-key/NOSTR_PRIVATE_KEY")
	}
	return nil, false, nil
}

func isProductionEnv() bool {
	env := strings.ToLower(strings.TrimSpace(os.Getenv("NOSTRIG_ENV")))
	return env == "production" || env == "prod"
}

func publicKeyFromSigner(ctx context.Context, signer nip34.Signer) (string, error) {
	provider, ok := signer.(nip34.PublicKeyProvider)
	if !ok {
		return "", fmt.Errorf("signer cannot provide a public key; set --claimer explicitly")
	}
	pub, err := provider.PublicKey(ctx)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(pub) == "" {
		return "", fmt.Errorf("signer returned empty public key")
	}
	return pub, nil
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

func maybeWaitForResponse(cmd *cobra.Command, relays []string, event *gonostr.Event, opts responseOptions) error {
	if !opts.wait {
		return nil
	}
	resp, err := taskfabric.WaitForContextVMResponse(cmd.Context(), relays, event, opts.timeout)
	if err != nil {
		return err
	}
	return json.NewEncoder(cmd.OutOrStdout()).Encode(resp)
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
