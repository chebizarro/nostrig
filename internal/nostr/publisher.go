package nostr

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	TaskStateSchema  = "cascadia.task-state.v1"
	TaskIntentSchema = "cascadia.task.v1"
)

type Signer interface {
	SignEvent(ctx context.Context, ev *gonostr.Event) error
}

type Publisher struct{ pool *gonostr.Pool }

func NewPublisher() *Publisher { return &Publisher{pool: gonostr.NewPool()} }

func (p *Publisher) Publish(ctx context.Context, relays []string, signer Signer, events []*gonostr.Event) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if p == nil || p.pool == nil {
		return fmt.Errorf("publisher is not initialized")
	}
	if len(relays) == 0 {
		return fmt.Errorf("no relays provided")
	}
	if signer == nil {
		return fmt.Errorf("signer is nil")
	}
	for _, ev := range events {
		if ev == nil {
			continue
		}
		if err := signer.SignEvent(ctx, ev); err != nil {
			return fmt.Errorf("sign event kind %d: %w", ev.Kind, err)
		}
		for _, relayURL := range relays {
			relay, err := p.pool.EnsureRelay(relayURL)
			if err != nil {
				return fmt.Errorf("connect relay %s: %w", relayURL, err)
			}
			if err := relay.Publish(ctx, *ev); err != nil {
				return fmt.Errorf("publish event kind %d to %s: %w", ev.Kind, relayURL, err)
			}
		}
	}
	return nil
}

func BuildCanonicalEvents(export *beadspb.Export, now time.Time) ([]*gonostr.Event, error) {
	if export == nil {
		return nil, fmt.Errorf("export is nil")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	events := make([]*gonostr.Event, 0, len(export.Issues)*2+len(export.Epics))
	for _, issue := range export.Issues {
		state, err := BuildTaskStateEvent(issue, now)
		if err != nil {
			return nil, err
		}
		events = append(events, state)
		if issueEvent := BuildNIP34IssueLinkEvent(issue, now); issueEvent != nil {
			events = append(events, issueEvent)
		}
	}
	for _, epic := range export.Epics {
		events = append(events, BuildEpicCollectionEvent(epic, export.Issues, now))
	}
	return events, nil
}

type TaskState struct {
	ID          string            `json:"id"`
	Title       string            `json:"title"`
	Description string            `json:"description,omitempty"`
	Status      string            `json:"status"`
	Priority    string            `json:"priority,omitempty"`
	Epic        string            `json:"epic,omitempty"`
	Assignee    string            `json:"assignee,omitempty"`
	Labels      []string          `json:"labels,omitempty"`
	DependsOn   []string          `json:"depends_on,omitempty"`
	Created     string            `json:"created,omitempty"`
	Updated     string            `json:"updated,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func BuildTaskStateEvent(issue *beadspb.Issue, now time.Time) (*gonostr.Event, error) {
	if issue == nil {
		return nil, fmt.Errorf("issue is nil")
	}
	id := strings.TrimSpace(issue.Id)
	if id == "" {
		return nil, fmt.Errorf("issue missing id")
	}
	state := TaskState{ID: id, Title: issue.Title, Description: issue.Description, Status: StatusString(issue.Status), Priority: PriorityString(issue.Priority), Epic: issue.Epic, Assignee: issue.Assignee, Labels: append([]string(nil), issue.Labels...), DependsOn: append([]string(nil), issue.DependsOn...), Metadata: metadataMap(issue.Metadata)}
	if issue.Created != nil {
		state.Created = issue.Created.AsTime().UTC().Format(time.RFC3339)
	}
	if issue.Updated != nil {
		state.Updated = issue.Updated.AsTime().UTC().Format(time.RFC3339)
	}
	content, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	tags := gonostr.Tags{{"d", "task:" + id}, {"domain", "task"}, {"schema", TaskStateSchema}, {"status", state.Status}}
	if state.Priority != "" {
		tags = append(tags, gonostr.Tag{"priority", state.Priority})
	}
	if state.Assignee != "" {
		tags = append(tags, gonostr.Tag{"assignee", state.Assignee})
	}
	if state.Epic != "" {
		tags = append(tags, gonostr.Tag{"a", "30000::epic:" + state.Epic}, gonostr.Tag{"epic", state.Epic})
	}
	for _, dep := range state.DependsOn {
		if strings.TrimSpace(dep) != "" {
			tags = append(tags, gonostr.Tag{"depends-on", "task:" + dep})
		}
	}
	for _, label := range state.Labels {
		if strings.TrimSpace(label) != "" {
			tags = append(tags, gonostr.Tag{"t", label})
		}
	}
	if v := state.Metadata["nostr.id"]; v != "" {
		tags = append(tags, gonostr.Tag{"e", v, "", "nip34-root"})
	}
	if v := state.Metadata["nip34.repo_addr"]; v != "" {
		tags = append(tags, gonostr.Tag{"a", v, "", "nip34-repo"})
	}
	return &gonostr.Event{Kind: gonostr.Kind(KindCanonicalState), CreatedAt: gonostr.Timestamp(now.Unix()), Tags: tags, Content: string(content)}, nil
}

func BuildNIP34IssueLinkEvent(issue *beadspb.Issue, now time.Time) *gonostr.Event {
	if issue == nil || issue.Metadata == nil || issue.Metadata.Custom == nil {
		return nil
	}
	repoAddr := strings.TrimSpace(issue.Metadata.Custom["nip34.repo_addr"])
	rootID := strings.TrimSpace(issue.Metadata.Custom["nostr.id"])
	if repoAddr == "" {
		return nil
	}
	tags := gonostr.Tags{{"a", repoAddr}, {"subject", issue.Title}, {"task", issue.Id}}
	if rootID != "" {
		tags = append(tags, gonostr.Tag{"e", rootID, "", "source"})
	}
	for _, label := range issue.Labels {
		if strings.TrimSpace(label) != "" {
			tags = append(tags, gonostr.Tag{"t", label})
		}
	}
	return &gonostr.Event{Kind: gonostr.Kind(KindIssue), CreatedAt: gonostr.Timestamp(now.Unix()), Tags: tags, Content: issue.Description}
}

func BuildNIP34IssueStatusEvent(issue *beadspb.Issue, now time.Time) *gonostr.Event {
	if issue == nil || issue.Metadata == nil || issue.Metadata.Custom == nil {
		return nil
	}
	repoAddr := strings.TrimSpace(issue.Metadata.Custom["nip34.repo_addr"])
	rootID := strings.TrimSpace(issue.Metadata.Custom["nostr.id"])
	if repoAddr == "" || rootID == "" {
		return nil
	}
	kind := KindStatusOpen
	switch issue.Status {
	case beadspb.Status_STATUS_CLOSED:
		kind = KindStatusClosed
	default:
		kind = KindStatusOpen
	}
	tags := gonostr.Tags{{"a", repoAddr}, {"e", rootID, "", "root"}, {"task", issue.Id}}
	return &gonostr.Event{Kind: gonostr.Kind(kind), CreatedAt: gonostr.Timestamp(now.Unix()), Tags: tags}
}

func BuildEpicCollectionEvent(epic *beadspb.Epic, issues []*beadspb.Issue, now time.Time) *gonostr.Event {
	id, name := "unknown", "unknown"
	if epic != nil {
		if strings.TrimSpace(epic.Id) != "" {
			id = epic.Id
		}
		if strings.TrimSpace(epic.Name) != "" {
			name = epic.Name
		} else {
			name = id
		}
	}
	tags := gonostr.Tags{{"d", "epic:" + id}, {"title", name}, {"schema", "cascadia.task-collection.v1"}}
	for _, issue := range issues {
		if issue != nil && issue.Epic == id {
			tags = append(tags, gonostr.Tag{"a", "30900::task:" + issue.Id})
		}
	}
	return &gonostr.Event{Kind: gonostr.Kind(KindNamedList), CreatedAt: gonostr.Timestamp(now.Unix()), Tags: tags}
}

func BuildQueueCollectionEvent(queue string, issueIDs []string, now time.Time) *gonostr.Event {
	queue = strings.TrimSpace(queue)
	if queue == "" {
		queue = "backlog"
	}
	tags := gonostr.Tags{{"d", "queue:" + queue}, {"title", queue}, {"schema", "cascadia.task-collection.v1"}}
	for _, id := range issueIDs {
		if strings.TrimSpace(id) != "" {
			tags = append(tags, gonostr.Tag{"a", "30900::task:" + id})
		}
	}
	return &gonostr.Event{Kind: gonostr.Kind(KindNamedList), CreatedAt: gonostr.Timestamp(now.Unix()), Tags: tags}
}

func ParseTaskStateEvent(ev *gonostr.Event) (*beadspb.Issue, error) {
	if ev == nil {
		return nil, fmt.Errorf("event is nil")
	}
	if ev.Kind != KindCanonicalState {
		return nil, fmt.Errorf("unexpected kind %d", ev.Kind)
	}
	d, ok := TagD(ev)
	if !ok || !strings.HasPrefix(d, "task:") {
		return nil, fmt.Errorf("task state missing d=task:<id>")
	}
	var state TaskState
	if err := json.Unmarshal([]byte(ev.Content), &state); err != nil {
		return nil, err
	}
	if state.ID == "" {
		state.ID = strings.TrimPrefix(d, "task:")
	}
	issue := &beadspb.Issue{Id: state.ID, Title: state.Title, Description: state.Description, Status: ParseStatus(state.Status), Priority: ParsePriority(state.Priority), Epic: state.Epic, Assignee: state.Assignee, Labels: append([]string(nil), state.Labels...), DependsOn: append([]string(nil), state.DependsOn...), Metadata: &beadspb.Metadata{Custom: state.Metadata}}
	if created, err := time.Parse(time.RFC3339, strings.TrimSpace(state.Created)); err == nil {
		issue.Created = timestamppb.New(created.UTC())
	}
	if updated, err := time.Parse(time.RFC3339, strings.TrimSpace(state.Updated)); err == nil {
		issue.Updated = timestamppb.New(updated.UTC())
	}
	return issue, nil
}

func BuildContextVMCommand(method, recipient string, params any, now time.Time) (*gonostr.Event, error) {
	parts := strings.Split(method, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("method must be <domain>/<op>")
	}
	id, err := json.Marshal(now.UnixNano())
	if err != nil {
		return nil, err
	}
	req, err := cascontextvm.NewRequest(id, cascontextvm.Method(parts[0], parts[1]), params)
	if err != nil {
		return nil, err
	}
	content, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	tags := gonostr.Tags{{"p", recipient}, {"method", req.Method}, {"domain", parts[0]}, {"op", parts[1]}, {"schema", TaskIntentSchema}}
	return &gonostr.Event{Kind: gonostr.Kind(KindContextVMIntent), CreatedAt: gonostr.Timestamp(now.Unix()), Tags: tags, Content: string(content)}, nil
}

func BuildClaimDispatch(taskID, claimer, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("task/claim", recipient, map[string]string{"task_id": taskID, "claimer": claimer, "dispatch": "fleet-worker"}, now)
}

func BuildAssignCommand(taskID, assignee, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("task/assign", recipient, map[string]string{"task_id": taskID, "assignee": assignee}, now)
}

func BuildCloseCommand(taskID, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("task/close", recipient, map[string]string{"task_id": taskID}, now)
}

func BuildQueueEnqueueCommand(queue, taskID, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("queue/enqueue", recipient, map[string]string{"queue": queue, "task_id": taskID}, now)
}

func BuildQueueDequeueCommand(queue, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("queue/dequeue", recipient, map[string]string{"queue": queue}, now)
}

func BuildQueueListCommand(queue, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("queue/list", recipient, map[string]string{"queue": queue}, now)
}

func StatusString(s beadspb.Status) string {
	switch s {
	case beadspb.Status_STATUS_IN_PROGRESS:
		return "in_progress"
	case beadspb.Status_STATUS_BLOCKED:
		return "blocked"
	case beadspb.Status_STATUS_CLOSED:
		return "closed"
	default:
		return "open"
	}
}
func ParseStatus(s string) beadspb.Status {
	switch strings.ToLower(s) {
	case "in_progress", "in-progress":
		return beadspb.Status_STATUS_IN_PROGRESS
	case "blocked":
		return beadspb.Status_STATUS_BLOCKED
	case "closed":
		return beadspb.Status_STATUS_CLOSED
	default:
		return beadspb.Status_STATUS_OPEN
	}
}
func PriorityString(p beadspb.Priority) string {
	switch p {
	case beadspb.Priority_PRIORITY_P0:
		return "P0"
	case beadspb.Priority_PRIORITY_P1:
		return "P1"
	case beadspb.Priority_PRIORITY_P2:
		return "P2"
	case beadspb.Priority_PRIORITY_P3:
		return "P3"
	case beadspb.Priority_PRIORITY_P4:
		return "P4"
	default:
		return ""
	}
}
func ParsePriority(s string) beadspb.Priority {
	switch strings.ToUpper(s) {
	case "P0":
		return beadspb.Priority_PRIORITY_P0
	case "P1":
		return beadspb.Priority_PRIORITY_P1
	case "P2":
		return beadspb.Priority_PRIORITY_P2
	case "P3":
		return beadspb.Priority_PRIORITY_P3
	case "P4":
		return beadspb.Priority_PRIORITY_P4
	default:
		return beadspb.Priority_PRIORITY_UNSPECIFIED
	}
}
func metadataMap(m *beadspb.Metadata) map[string]string {
	out := map[string]string{}
	if m != nil {
		for k, v := range m.Custom {
			out[k] = v
		}
	}
	return out
}
