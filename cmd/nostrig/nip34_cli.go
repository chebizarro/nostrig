package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	pb "github.com/chebizarro/nostrig/gen/beads"
	"github.com/chebizarro/nostrig/internal/gitea"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"github.com/chebizarro/nostrig/internal/reconcile"
	"github.com/chebizarro/nostrig/internal/taskfabric"
	"github.com/chebizarro/nostrig/internal/taskmodel"
	"github.com/spf13/cobra"
)

func newNIP34Cmd() *cobra.Command {
	cmd := &cobra.Command{Use: "nip34", Short: "Inspect and repair NIP-34, Nostrig, and Gitea synchronization"}
	cmd.AddCommand(newNIP34ReconcileCmd())
	return cmd
}

func newNIP34ReconcileCmd() *cobra.Command {
	var repoAddr, author, giteaURL, giteaRepo, tokenFile string
	var relays, links []string
	var repair bool
	var signing signingOptions
	cmd := &cobra.Command{
		Use:   "reconcile",
		Short: "Report or repair NIP-34/Nostrig/Gitea drift",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if repair && signing.dryRun {
				return fmt.Errorf("--repair and --dry-run are mutually exclusive; omit --repair for report-only mode")
			}
			repoAddr = strings.TrimSpace(repoAddr)
			if _, err := nip34.ParseRepositoryAddress(repoAddr); err != nil {
				return err
			}
			authorKey, err := gonostr.PubKeyFromHex(strings.TrimSpace(author))
			if err != nil {
				return fmt.Errorf("--author must be a canonical task author hex pubkey")
			}
			author = authorKey.Hex()
			relays = relaysWithEnv(relays)
			if len(relays) == 0 {
				return fmt.Errorf("at least one --relay or NOSTR_RELAY is required")
			}
			token, err := loadGiteaToken(tokenFile)
			if err != nil {
				return err
			}
			linkRepoOwner, linkRepoName, err := splitGiteaRepo(giteaRepo, len(links) > 0)
			if err != nil {
				return err
			}
			explicit := map[string]int64{}
			for _, value := range links {
				taskID, number, err := reconcile.ParseLinkSpec(value)
				if err != nil {
					return err
				}
				if _, duplicate := explicit[taskID]; duplicate {
					return fmt.Errorf("duplicate --link for task %s", taskID)
				}
				explicit[taskID] = number
			}
			if len(explicit) > 0 && strings.TrimSpace(giteaURL) == "" {
				return fmt.Errorf("--gitea-url is required with --link")
			}

			client := nip34.NewClient()
			events, err := taskfabric.FetchTaskStateEvents(cmd.Context(), client, taskfabric.SyncOptions{
				Relays: relays, RepoAddr: repoAddr, Authors: []string{author},
			})
			if err != nil {
				return err
			}
			export, err := taskfabric.ExportFromTaskStateEvents(events)
			if err != nil {
				return err
			}
			taskEvents := latestTaskEventIDs(events)
			repo, err := nip34.ResolveRepositoryAnnouncement(cmd.Context(), client, relays, repoAddr)
			if err != nil {
				return err
			}
			trusted, err := nip34.TrustedMaintainers(repo)
			if err != nil {
				return err
			}

			rootIDs := make([]string, 0, len(export.Issues))
			for _, task := range export.Issues {
				if task != nil {
					if root := strings.TrimSpace(task.GetMetadata().GetCustom()["nostr.id"]); root != "" {
						rootIDs = append(rootIDs, root)
					}
				}
			}
			statusEvents, err := fetchNIP34Statuses(cmd.Context(), client, relays, rootIDs)
			if err != nil {
				return err
			}
			statuses := nip34.ResolveStatuses(statusEvents, rootIDs, trusted)

			items := make([]reconcile.Item, 0, len(export.Issues))
			claimedGiteaIssues := map[string]string{}
			var configuredGitea *gitea.Client
			configuredGiteaBase := strings.TrimRight(strings.TrimSpace(giteaURL), "/")
			if configuredGiteaBase != "" {
				configuredGitea, err = gitea.NewClient(configuredGiteaBase, token, nil)
				if err != nil {
					return err
				}
			}
			for _, task := range export.Issues {
				if task == nil {
					continue
				}
				if number, ok := explicit[task.Id]; ok {
					if err := reconcile.LinkIssue(task, giteaURL, linkRepoOwner, linkRepoName, number); err != nil {
						return err
					}
				}
				link, linked, err := taskmodel.ParseGiteaLink(task.GetMetadata().GetCustom())
				if err != nil {
					return fmt.Errorf("task %s: %w", task.Id, err)
				}
				var external *gitea.Issue
				if linked {
					if previousTask := claimedGiteaIssues[link.IssueURL]; previousTask != "" && previousTask != task.Id {
						return fmt.Errorf("Gitea issue %s is linked to both %s and %s", link.IssueURL, previousTask, task.Id)
					}
					claimedGiteaIssues[link.IssueURL] = task.Id
					if configuredGitea == nil {
						configuredGiteaBase = link.BaseURL
						configuredGitea, err = gitea.NewClient(link.BaseURL, token, nil)
						if err != nil {
							return err
						}
					}
					if configuredGiteaBase != link.BaseURL {
						return fmt.Errorf("task %s Gitea base URL %s does not match configured %s", task.Id, link.BaseURL, configuredGiteaBase)
					}
					external, err = configuredGitea.GetIssue(cmd.Context(), gitea.IssueKey{Owner: link.Owner, Repo: link.Repo, Number: link.IssueNumber})
					if err != nil && !gitea.IsNotFound(err) {
						return fmt.Errorf("task %s: %w", task.Id, err)
					}
					if gitea.IsNotFound(err) {
						external = nil
					}
				}
				root := task.GetMetadata().GetCustom()["nostr.id"]
				items = append(items, reconcile.Item{
					Task: task, TaskEventID: taskEvents[task.Id], Status: statuses.Trusted[root],
					UntrustedStatusCount: len(statuses.Untrusted[root]), Gitea: external,
				})
			}

			var signer nip34.Signer
			if repair {
				signer, _, err = signerFromOptions(cmd.Context(), signing, true)
				if err != nil {
					return err
				}
				signerAuthor, err := publicKeyFromSigner(cmd.Context(), signer)
				if err != nil {
					return err
				}
				if strings.ToLower(signerAuthor) != author {
					return fmt.Errorf("repair signer must match canonical --author")
				}
				if !nip34.IsTrustedMaintainer(repo, author) {
					return fmt.Errorf("canonical author %s is not a trusted repository maintainer", author)
				}
			}
			writer := &nip34RepairWriter{
				client: client, relays: relays, author: author, signer: signer,
				publisher: nip34.NewPublisher(), gitea: configuredGitea,
			}
			engine := &reconcile.Reconciler{Gitea: writer, NIP34: writer, Tasks: writer}
			report, err := engine.Reconcile(cmd.Context(), items, repair)
			if err != nil {
				return err
			}
			encoder := json.NewEncoder(cmd.OutOrStdout())
			encoder.SetIndent("", "  ")
			if err := encoder.Encode(report); err != nil {
				return err
			}
			if report.Summary.Drifted > 0 || report.Summary.Unrepairable > 0 || report.Summary.Failed > 0 {
				return fmt.Errorf("reconciliation drift remains")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&repoAddr, "repo-addr", "", "NIP-34 repository address (required)")
	cmd.Flags().StringVar(&author, "author", "", "Canonical task-state author pubkey (required)")
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL (repeatable)")
	cmd.Flags().StringVar(&giteaURL, "gitea-url", "", "Gitea base URL")
	cmd.Flags().StringVar(&giteaRepo, "gitea-repo", "", "Gitea owner/repository used by --link")
	cmd.Flags().StringVar(&tokenFile, "gitea-token-file", "", "File containing the Gitea API token")
	cmd.Flags().StringSliceVar(&links, "link", nil, "Stable task-to-issue mapping task-id=issue-number (repeatable)")
	cmd.Flags().BoolVar(&repair, "repair", false, "Apply authoritative repairs; default is report-only")
	_ = cmd.MarkFlagRequired("repo-addr")
	_ = cmd.MarkFlagRequired("author")
	addSigningFlags(cmd, &signing)
	return cmd
}

type nip34RepairWriter struct {
	client    *nip34.Client
	relays    []string
	author    string
	signer    nip34.Signer
	publisher taskfabric.EventPublisher
	gitea     *gitea.Client
}

func (w *nip34RepairWriter) UpdateIssueState(ctx context.Context, key gitea.IssueKey, state, etag string) (*gitea.Issue, error) {
	if w == nil || w.gitea == nil {
		return nil, fmt.Errorf("Gitea client is not configured")
	}
	return w.gitea.UpdateIssueState(ctx, key, state, etag)
}

func (w *nip34RepairWriter) UpdateNIP34Status(ctx context.Context, task *pb.Issue, kind int, revision string) error {
	event := nip34.BuildNIP34IssueStatusEventForKind(task, kind, revision, time.Now().UTC())
	if event == nil {
		return fmt.Errorf("task %s has no valid NIP-34 status link", task.GetId())
	}
	return w.publisher.Publish(ctx, w.relays, w.signer, []*gonostr.Event{event})
}

func (w *nip34RepairWriter) UpdateTaskState(ctx context.Context, task *pb.Issue, baseEventID string) error {
	events, err := taskfabric.FetchTaskStateEvents(ctx, w.client, taskfabric.SyncOptions{
		Relays: w.relays, TaskIDs: []string{task.Id}, Authors: []string{w.author},
	})
	if err != nil {
		return err
	}
	if current := latestTaskEventIDs(events)[task.Id]; current != baseEventID {
		return fmt.Errorf("canonical task %s changed concurrently (expected %s, found %s)", task.Id, baseEventID, current)
	}
	event, err := nip34.BuildTaskStateEvent(task, w.author, time.Now().UTC())
	if err != nil {
		return err
	}
	return w.publisher.Publish(ctx, w.relays, w.signer, []*gonostr.Event{event})
}

func fetchNIP34Statuses(ctx context.Context, client *nip34.Client, relays, rootIDs []string) ([]*gonostr.Event, error) {
	rootIDs = uniqueStrings(rootIDs)
	if len(rootIDs) == 0 {
		return nil, nil
	}
	filters := make([]gonostr.Filter, 0, (len(rootIDs)+199)/200)
	for len(rootIDs) > 0 {
		n := 200
		if len(rootIDs) < n {
			n = len(rootIDs)
		}
		filters = append(filters, gonostr.Filter{
			Kinds: []gonostr.Kind{
				gonostr.Kind(nip34.KindStatusOpen), gonostr.Kind(nip34.KindStatusApplied),
				gonostr.Kind(nip34.KindStatusClosed), gonostr.Kind(nip34.KindStatusDraft),
			},
			Tags: gonostr.TagMap{"e": append([]string(nil), rootIDs[:n]...)},
		})
		rootIDs = rootIDs[n:]
	}
	return client.FetchManyPaginated(ctx, relays, filters, nip34.PaginationOptions{})
}

func latestTaskEventIDs(events []*gonostr.Event) map[string]string {
	type candidate struct {
		event   *gonostr.Event
		version nip34.TaskStateSchemaVersion
	}
	latest := map[string]candidate{}
	for _, event := range events {
		if event == nil || int(event.Kind) != nip34.KindCanonicalState {
			continue
		}
		issue, version, err := nip34.ParseTaskStateEventVersioned(event)
		if err != nil || issue == nil || strings.TrimSpace(issue.Id) == "" {
			continue
		}
		previous, exists := latest[issue.Id]
		if !exists || version > previous.version ||
			(version == previous.version && (event.CreatedAt > previous.event.CreatedAt ||
				(event.CreatedAt == previous.event.CreatedAt && event.ID.Hex() > previous.event.ID.Hex()))) {
			latest[issue.Id] = candidate{event: event, version: version}
		}
	}
	out := make(map[string]string, len(latest))
	for taskID, candidate := range latest {
		out[taskID] = candidate.event.ID.Hex()
	}
	return out
}

func loadGiteaToken(flag string) (string, error) {
	path := strings.TrimSpace(flag)
	if path == "" {
		path = strings.TrimSpace(os.Getenv("NOSTRIG_GITEA_TOKEN_FILE"))
	}
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read Gitea token file: %w", err)
		}
		return strings.TrimSpace(string(raw)), nil
	}
	token := strings.TrimSpace(os.Getenv("NOSTRIG_GITEA_TOKEN"))
	if token != "" && strings.EqualFold(strings.TrimSpace(os.Getenv("NOSTRIG_ENV")), "production") {
		return "", fmt.Errorf("NOSTRIG_GITEA_TOKEN is forbidden in production; use --gitea-token-file")
	}
	return token, nil
}

func splitGiteaRepo(value string, required bool) (string, string, error) {
	value = strings.Trim(strings.TrimSpace(value), "/")
	if value == "" && !required {
		return "", "", nil
	}
	parts := strings.Split(value, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("--gitea-repo must be owner/repository")
	}
	return parts[0], parts[1], nil
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
