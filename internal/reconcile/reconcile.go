package reconcile

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	pb "github.com/chebizarro/nostrig/gen/beads"
	"github.com/chebizarro/nostrig/internal/gitea"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"github.com/chebizarro/nostrig/internal/taskmodel"
	"google.golang.org/protobuf/proto"
)

const ReportSchema = "nostrig.nip34-reconcile.v1"

type GiteaStateWriter interface {
	UpdateIssueState(context.Context, gitea.IssueKey, string, string) (*gitea.Issue, error)
}

type NIP34StatusWriter interface {
	UpdateNIP34Status(context.Context, *pb.Issue, int, string) error
}

type TaskStateWriter interface {
	UpdateTaskState(context.Context, *pb.Issue, string) error
}

type Reconciler struct {
	Gitea GiteaStateWriter
	NIP34 NIP34StatusWriter
	Tasks TaskStateWriter
}

type Item struct {
	Task                 *pb.Issue
	TaskEventID          string
	Status               *nip34.StatusEvent
	UntrustedStatusCount int
	Gitea                *gitea.Issue
}

type Drift struct {
	Field     string `json:"field"`
	Authority string `json:"authority"`
	Target    string `json:"target"`
	Actual    string `json:"actual,omitempty"`
	Desired   string `json:"desired,omitempty"`
	Action    string `json:"action"`
}

type ItemReport struct {
	TaskID        string   `json:"task_id"`
	TaskEventID   string   `json:"task_event_id,omitempty"`
	NIP34RootID   string   `json:"nip34_root_id,omitempty"`
	GiteaIssueURL string   `json:"gitea_issue_url,omitempty"`
	State         string   `json:"state"`
	Drift         []Drift  `json:"drift,omitempty"`
	Warnings      []string `json:"warnings,omitempty"`
	Actions       []string `json:"actions,omitempty"`
	Error         string   `json:"error,omitempty"`
}

type Summary struct {
	InSync       int `json:"in_sync"`
	Drifted      int `json:"drifted"`
	Repaired     int `json:"repaired"`
	Unrepairable int `json:"unrepairable"`
	Failed       int `json:"failed"`
}

type Report struct {
	Schema  string       `json:"schema"`
	Repair  bool         `json:"repair"`
	Items   []ItemReport `json:"items"`
	Summary Summary      `json:"summary"`
}

func (r *Reconciler) Reconcile(ctx context.Context, items []Item, repair bool) (*Report, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	sorted := append([]Item(nil), items...)
	sort.Slice(sorted, func(i, j int) bool {
		left, right := "", ""
		if sorted[i].Task != nil {
			left = sorted[i].Task.Id
		}
		if sorted[j].Task != nil {
			right = sorted[j].Task.Id
		}
		return left < right
	})
	report := &Report{Schema: ReportSchema, Repair: repair, Items: make([]ItemReport, 0, len(sorted))}
	for _, item := range sorted {
		result := r.reconcileItem(ctx, item, repair)
		report.Items = append(report.Items, result)
		switch result.State {
		case "in_sync":
			report.Summary.InSync++
		case "drift":
			report.Summary.Drifted++
		case "repaired":
			report.Summary.Repaired++
		case "unrepairable", "unlinked_gitea":
			report.Summary.Unrepairable++
		default:
			report.Summary.Failed++
		}
	}
	return report, nil
}

func (r *Reconciler) reconcileItem(ctx context.Context, item Item, repair bool) ItemReport {
	out := ItemReport{State: "in_sync", TaskEventID: item.TaskEventID}
	if item.Task == nil {
		out.State, out.Error = "failed", "task is required"
		return out
	}
	out.TaskID = item.Task.Id
	metadata := item.Task.GetMetadata().GetCustom()
	rootID := strings.TrimSpace(metadata["nostr.id"])
	out.NIP34RootID = rootID
	link, linked, err := taskmodel.ParseGiteaLink(metadata)
	if err != nil {
		out.State, out.Error = "failed", err.Error()
		return out
	}
	if linked {
		out.GiteaIssueURL = link.IssueURL
	}
	if item.UntrustedStatusCount > 0 {
		out.Warnings = append(out.Warnings, "untrusted_nip34_status")
	}

	desiredState := externalState(item.Task)
	desiredStatusKind := nip34.KindStatusOpen
	if desiredState == "closed" {
		desiredStatusKind = nip34.KindStatusClosed
	}
	currentStatusKind := 0
	if item.Status != nil {
		currentStatusKind = item.Status.Kind
	}
	if currentStatusKind != desiredStatusKind {
		out.Drift = append(out.Drift, Drift{
			Field: "status", Authority: "nostrig", Target: "nip34",
			Actual: statusName(currentStatusKind), Desired: statusName(desiredStatusKind), Action: "publish_nip34_status",
		})
	}

	working := proto.Clone(item.Task).(*pb.Issue)
	ensureMetadata(working)
	importedGitea := false
	if linked {
		if item.Gitea == nil {
			out.Drift = append(out.Drift, Drift{Field: "issue", Authority: "gitea", Target: "nostrig", Actual: "missing", Desired: link.IssueURL, Action: "restore_or_relink_gitea_issue"})
		} else {
			if strings.TrimSpace(working.Title) != strings.TrimSpace(item.Gitea.Title) {
				out.Drift = append(out.Drift, Drift{Field: "title", Authority: "gitea", Target: "nostrig", Actual: working.Title, Desired: item.Gitea.Title, Action: "update_nostrig_task"})
				working.Title = strings.TrimSpace(item.Gitea.Title)
				importedGitea = true
			}
			if strings.TrimSpace(working.Description) != strings.TrimSpace(item.Gitea.Body) {
				out.Drift = append(out.Drift, Drift{Field: "description", Authority: "gitea", Target: "nostrig", Actual: working.Description, Desired: item.Gitea.Body, Action: "update_nostrig_task"})
				working.Description = strings.TrimSpace(item.Gitea.Body)
				importedGitea = true
			}
			if strings.ToLower(strings.TrimSpace(item.Gitea.State)) != desiredState {
				out.Drift = append(out.Drift, Drift{Field: "status", Authority: "nostrig", Target: "gitea", Actual: item.Gitea.State, Desired: desiredState, Action: "update_gitea_state"})
			}
		}
	} else {
		out.Warnings = append(out.Warnings, "unlinked_gitea")
	}

	nipRevision := revision(map[string]string{
		"root_id": rootID, "root_kind": metadata["nostr.kind"], "status": statusName(desiredStatusKind),
	})
	giteaRevision := ""
	if linked && item.Gitea != nil {
		giteaRevision = revision(map[string]string{
			"url": link.IssueURL, "title": strings.TrimSpace(item.Gitea.Title),
			"body": strings.TrimSpace(item.Gitea.Body), "state": desiredState,
		})
	}
	doc, err := taskmodel.FromProto(working)
	if err != nil {
		out.State, out.Error = "failed", err.Error()
		return out
	}
	taskRevision := taskmodel.MaterialRevision(doc)
	origin := strings.TrimSpace(working.Metadata.Custom["sync.origin"])
	originRevision := strings.TrimSpace(working.Metadata.Custom["sync.origin_revision"])
	if importedGitea {
		origin, originRevision = "gitea", giteaRevision
	} else if origin == "" {
		origin, originRevision = "nostrig", taskRevision
	}
	desiredSync := map[string]string{
		"sync.origin":                     origin,
		"sync.origin_revision":            originRevision,
		"sync.nostrig.source_revision":    taskRevision,
		"sync.nostrig.last_sync_revision": taskRevision,
		"sync.nip34.source_revision":      nipRevision,
		"sync.nip34.last_sync_revision":   nipRevision,
	}
	if linked && item.Gitea != nil {
		desiredSync["sync.gitea.source_revision"] = giteaRevision
		desiredSync["sync.gitea.last_sync_revision"] = giteaRevision
	}
	syncChanged := false
	for key, value := range desiredSync {
		if working.Metadata.Custom[key] != value {
			syncChanged = true
			working.Metadata.Custom[key] = value
		}
	}
	if syncChanged {
		out.Drift = append(out.Drift, Drift{Field: "sync_revisions", Authority: "reconciler", Target: "nostrig", Action: "record_sync_revisions"})
	}

	sort.Slice(out.Drift, func(i, j int) bool {
		if out.Drift[i].Target == out.Drift[j].Target {
			return out.Drift[i].Field < out.Drift[j].Field
		}
		return out.Drift[i].Target < out.Drift[j].Target
	})
	if len(out.Drift) == 0 {
		return out
	}
	out.State = "drift"
	if !repair {
		return out
	}
	if linked && item.Gitea == nil {
		out.State = "unrepairable"
		return out
	}
	if linked && item.Gitea != nil && strings.ToLower(strings.TrimSpace(item.Gitea.State)) != desiredState {
		if r == nil || r.Gitea == nil {
			out.State, out.Error = "unrepairable", "Gitea repair writer is not configured"
			return out
		}
		updated, err := r.Gitea.UpdateIssueState(ctx, gitea.IssueKey{Owner: link.Owner, Repo: link.Repo, Number: link.IssueNumber}, desiredState, item.Gitea.ETag)
		if err != nil {
			out.State, out.Error = "failed", err.Error()
			return out
		}
		item.Gitea = updated
		out.Actions = append(out.Actions, "updated_gitea_state")
	}
	if currentStatusKind != desiredStatusKind {
		if r == nil || r.NIP34 == nil {
			out.State, out.Error = "unrepairable", "NIP-34 repair writer is not configured"
			return out
		}
		if err := r.NIP34.UpdateNIP34Status(ctx, working, desiredStatusKind, nipRevision); err != nil {
			out.State, out.Error = "failed", err.Error()
			return out
		}
		out.Actions = append(out.Actions, "published_nip34_status")
	}
	if importedGitea || syncChanged {
		if r == nil || r.Tasks == nil {
			out.State, out.Error = "unrepairable", "task repair writer is not configured"
			return out
		}
		if err := r.Tasks.UpdateTaskState(ctx, working, item.TaskEventID); err != nil {
			out.State, out.Error = "failed", err.Error()
			return out
		}
		out.Actions = append(out.Actions, "updated_nostrig_task")
	}
	out.State = "repaired"
	return out
}

func ensureMetadata(issue *pb.Issue) {
	if issue.Metadata == nil {
		issue.Metadata = &pb.Metadata{}
	}
	if issue.Metadata.Custom == nil {
		issue.Metadata.Custom = map[string]string{}
	}
}

func externalState(issue *pb.Issue) string {
	if issue != nil && issue.Status == pb.Status_STATUS_CLOSED {
		return "closed"
	}
	return "open"
}

func statusName(kind int) string {
	switch kind {
	case nip34.KindStatusOpen:
		return "open"
	case nip34.KindStatusApplied:
		return "applied"
	case nip34.KindStatusClosed:
		return "closed"
	case nip34.KindStatusDraft:
		return "draft"
	default:
		return "missing"
	}
}

func revision(value any) string {
	return taskmodel.StableRevision(value)
}

// SyncMetadata returns the recorded per-side source and last-synchronized
// revisions for diagnostics and CLI rendering.
func SyncMetadata(issue *pb.Issue) map[string]string {
	out := map[string]string{}
	if issue == nil || issue.Metadata == nil {
		return out
	}
	for key, value := range issue.Metadata.Custom {
		if strings.HasPrefix(key, "sync.") {
			out[key] = value
		}
	}
	return out
}

func LinkIssue(issue *pb.Issue, baseURL, owner, repo string, number int64) error {
	if issue == nil || number <= 0 {
		return fmt.Errorf("task and positive issue number are required")
	}
	ensureMetadata(issue)
	return taskmodel.SetGiteaLink(issue.Metadata.Custom, taskmodel.GiteaLink{
		BaseURL: baseURL, Owner: owner, Repo: repo, IssueNumber: number,
	})
}

func ParseLinkSpec(value string) (string, int64, error) {
	parts := strings.SplitN(strings.TrimSpace(value), "=", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
		return "", 0, fmt.Errorf("link must be task-id=issue-number")
	}
	number, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || number <= 0 {
		return "", 0, fmt.Errorf("link must contain a positive issue number")
	}
	return strings.TrimSpace(parts[0]), number, nil
}
