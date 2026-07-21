package nostr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	TaskStateSchema     = "cascadia.task-state.v1"
	TaskIntentSchema    = "cascadia.task.v1"
	TaskTombstoneSchema = "cascadia.task-tombstone.v1"
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

func BuildCanonicalEvents(export *beadspb.Export, canonicalAuthor string, now time.Time) ([]*gonostr.Event, error) {
	if export == nil {
		return nil, fmt.Errorf("export is nil")
	}
	canonicalAuthor, err := canonicalPubKey(canonicalAuthor)
	if err != nil {
		return nil, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	events := make([]*gonostr.Event, 0, len(export.Issues)*2+len(export.Epics))
	for _, issue := range export.Issues {
		state, err := BuildTaskStateEvent(issue, canonicalAuthor, now)
		if err != nil {
			return nil, err
		}
		events = append(events, state)
		if issueEvent := BuildNIP34IssueLinkEvent(issue, now); issueEvent != nil {
			events = append(events, issueEvent)
		}
	}
	for _, epic := range export.Epics {
		events = append(events, BuildEpicCollectionEvent(epic, export.Issues, canonicalAuthor, now))
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

func BuildTaskStateEvent(issue *beadspb.Issue, canonicalAuthor string, now time.Time) (*gonostr.Event, error) {
	canonicalAuthor, err := canonicalPubKey(canonicalAuthor)
	if err != nil {
		return nil, err
	}
	if issue == nil {
		return nil, fmt.Errorf("issue is nil")
	}
	id := strings.TrimSpace(issue.Id)
	if id == "" {
		return nil, fmt.Errorf("issue missing id")
	}
	if strings.TrimSpace(issue.Title) == "" {
		return nil, fmt.Errorf("issue missing title")
	}
	if !sameStrings(issue.Labels, issue.Labels) {
		return nil, fmt.Errorf("issue labels must be unique and non-empty")
	}
	if !sameStrings(issue.DependsOn, issue.DependsOn) {
		return nil, fmt.Errorf("issue dependencies must be unique and non-empty")
	}
	for _, dependency := range issue.DependsOn {
		if strings.HasPrefix(strings.TrimSpace(dependency), "task:") {
			return nil, fmt.Errorf("issue dependency ids must be unprefixed")
		}
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
		tags = append(tags, gonostr.Tag{"a", Address(KindNamedList, canonicalAuthor, "epic:"+state.Epic)}, gonostr.Tag{"epic", state.Epic})
	}
	for _, dep := range state.DependsOn {
		if strings.TrimSpace(dep) != "" {
			tags = append(tags, gonostr.Tag{"depends-on", Address(KindCanonicalState, canonicalAuthor, "task:"+dep)})
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

func BuildEpicCollectionEvent(epic *beadspb.Epic, issues []*beadspb.Issue, canonicalAuthor string, now time.Time) *gonostr.Event {
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
			tags = append(tags, gonostr.Tag{"a", Address(KindCanonicalState, canonicalAuthor, "task:"+issue.Id)})
		}
	}
	return &gonostr.Event{Kind: gonostr.Kind(KindNamedList), CreatedAt: gonostr.Timestamp(now.Unix()), Tags: tags}
}

type QueueReservation struct {
	TaskID    string
	Worker    string
	LeaseID   string
	ExpiresAt time.Time
}

func BuildQueueCollectionEventForAuthor(repoAddr, queue string, issueIDs []string, canonicalAuthor string, now time.Time) *gonostr.Event {
	return BuildQueueCollectionEventWithReservations(repoAddr, queue, issueIDs, nil, canonicalAuthor, now)
}

func BuildQueueCollectionEventWithReservations(repoAddr, queue string, issueIDs []string, reservations []QueueReservation, canonicalAuthor string, now time.Time) *gonostr.Event {
	queue = strings.TrimSpace(queue)
	if queue == "" {
		queue = "backlog"
	}
	author := strings.ToLower(strings.TrimSpace(canonicalAuthor))
	d := "queue:" + strings.TrimSpace(repoAddr) + ":" + queue
	tags := gonostr.Tags{{"d", d}, {"title", queue}, {"schema", "cascadia.task-collection.v1"}, {"a", strings.TrimSpace(repoAddr), "", "nip34-repo"}}
	for _, id := range issueIDs {
		if id = strings.TrimSpace(id); id != "" {
			tags = append(tags, gonostr.Tag{"a", Address(KindCanonicalState, author, "task:"+id)})
		}
	}
	for _, reservation := range reservations {
		if reservation.TaskID == "" || reservation.Worker == "" || reservation.LeaseID == "" || reservation.ExpiresAt.IsZero() {
			continue
		}
		tags = append(tags, gonostr.Tag{"lease", reservation.TaskID, reservation.Worker, reservation.ExpiresAt.UTC().Format(time.RFC3339Nano), reservation.LeaseID})
	}
	return &gonostr.Event{Kind: gonostr.Kind(KindNamedList), CreatedAt: gonostr.Timestamp(now.Unix()), Tags: tags}
}

func BuildTaskTombstone(target *gonostr.Event, repoAddr, canonicalAuthor string, now time.Time) (*gonostr.Event, error) {
	canonicalAuthor, err := canonicalPubKey(canonicalAuthor)
	if err != nil {
		return nil, err
	}
	if target == nil || target.Kind != gonostr.Kind(KindCanonicalState) {
		return nil, fmt.Errorf("canonical task state target is required")
	}
	if target.PubKey.Hex() != canonicalAuthor {
		return nil, fmt.Errorf("cannot delete task state authored by another key")
	}
	d, err := exactlyOneTag(target, "d")
	if err != nil || !strings.HasPrefix(d, "task:") {
		return nil, fmt.Errorf("invalid task state target")
	}
	repoAddr = strings.TrimSpace(repoAddr)
	if repoAddr == "" {
		return nil, fmt.Errorf("repo addr is required")
	}
	tags := gonostr.Tags{
		{"e", target.ID.Hex()},
		{"k", fmt.Sprintf("%d", KindCanonicalState)},
		{"a", Address(KindCanonicalState, canonicalAuthor, d), "", "task"},
		{"a", repoAddr, "", "nip34-repo"},
		{"schema", TaskTombstoneSchema},
	}
	return &gonostr.Event{Kind: gonostr.Kind(5), CreatedAt: gonostr.Timestamp(now.Unix()), Tags: tags, Content: "delete " + d}, nil
}

func TaskTombstoneRepo(ev *gonostr.Event) string {
	return markedTag(ev, "a", "nip34-repo")
}

func ValidateTaskTombstone(ev *gonostr.Event, canonicalAuthor string) (string, error) {
	canonicalAuthor, err := canonicalPubKey(canonicalAuthor)
	if err != nil {
		return "", err
	}
	if ev == nil || ev.Kind != gonostr.Kind(5) || ev.PubKey.Hex() != canonicalAuthor {
		return "", fmt.Errorf("invalid canonical task tombstone author")
	}
	if schema, err := exactlyOneTag(ev, "schema"); err != nil || schema != TaskTombstoneSchema {
		return "", fmt.Errorf("invalid task tombstone schema")
	}
	if kind, err := exactlyOneTag(ev, "k"); err != nil || kind != fmt.Sprintf("%d", KindCanonicalState) {
		return "", fmt.Errorf("invalid task tombstone kind")
	}
	if _, err := exactlyOneTag(ev, "e"); err != nil {
		return "", fmt.Errorf("task tombstone requires target event")
	}
	var taskD string
	for _, tag := range ev.Tags {
		if len(tag) >= 4 && tag[0] == "a" && tag[3] == "task" {
			prefix := fmt.Sprintf("%d:%s:", KindCanonicalState, canonicalAuthor)
			if taskD != "" || !strings.HasPrefix(tag[1], prefix+"task:") {
				return "", fmt.Errorf("invalid task tombstone coordinate")
			}
			taskD = strings.TrimPrefix(tag[1], prefix)
		}
	}
	if taskD == "" || markedTag(ev, "a", "nip34-repo") == "" {
		return "", fmt.Errorf("task tombstone missing canonical coordinates")
	}
	return strings.TrimPrefix(taskD, "task:"), nil
}

func canonicalPubKey(author string) (string, error) {
	author = strings.ToLower(strings.TrimSpace(author))
	if _, err := gonostr.PubKeyFromHex(author); err != nil {
		return "", fmt.Errorf("canonical author must be a valid pubkey")
	}
	return author, nil
}

func ParseTaskStateEvent(ev *gonostr.Event) (*beadspb.Issue, error) {
	if ev == nil {
		return nil, fmt.Errorf("event is nil")
	}
	if ev.Kind != KindCanonicalState {
		return nil, fmt.Errorf("unexpected kind %d", ev.Kind)
	}
	d, err := exactlyOneTag(ev, "d")
	if err != nil || !strings.HasPrefix(d, "task:") || strings.TrimPrefix(d, "task:") == "" {
		return nil, fmt.Errorf("task state requires exactly one d=task:<id>")
	}
	if schema, err := exactlyOneTag(ev, "schema"); err != nil || schema != TaskStateSchema {
		return nil, fmt.Errorf("unsupported task state schema")
	}
	if domain, err := exactlyOneTag(ev, "domain"); err != nil || domain != "task" {
		return nil, fmt.Errorf("task state requires domain=task")
	}
	var state TaskState
	dec := json.NewDecoder(bytes.NewBufferString(ev.Content))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&state); err != nil {
		return nil, fmt.Errorf("decode task state: %w", err)
	}
	if err := ensureDecoderEOF(dec); err != nil {
		return nil, err
	}
	id := strings.TrimPrefix(d, "task:")
	if strings.TrimSpace(state.ID) != state.ID || state.ID == "" || state.ID != id {
		return nil, fmt.Errorf("task content id does not match d tag")
	}
	if strings.TrimSpace(state.Title) == "" {
		return nil, fmt.Errorf("task title is required")
	}
	if !validTaskStatus(state.Status) {
		return nil, fmt.Errorf("invalid task status")
	}
	if state.Priority != "" && ParsePriority(state.Priority) == beadspb.Priority_PRIORITY_UNSPECIFIED {
		return nil, fmt.Errorf("invalid task priority")
	}
	if err := requireOptionalTagAgreement(ev, "status", state.Status); err != nil {
		return nil, err
	}
	if err := requireOptionalTagAgreement(ev, "priority", state.Priority); err != nil {
		return nil, err
	}
	if err := requireOptionalTagAgreement(ev, "assignee", state.Assignee); err != nil {
		return nil, err
	}
	author := ev.PubKey.Hex()
	if state.Epic == "" {
		if len(TagAll(ev, "epic")) != 0 {
			return nil, fmt.Errorf("epic tag does not match content")
		}
	} else {
		if err := requireOptionalTagAgreement(ev, "epic", state.Epic); err != nil {
			return nil, err
		}
		if !hasExactTag(ev, "a", Address(KindNamedList, author, "epic:"+state.Epic)) {
			return nil, fmt.Errorf("epic coordinate does not identify canonical author")
		}
	}
	if !sameStrings(TagAll(ev, "t"), state.Labels) {
		return nil, fmt.Errorf("label tags do not match content")
	}
	expectedDeps := make([]string, 0, len(state.DependsOn))
	for _, dep := range state.DependsOn {
		dep = strings.TrimSpace(dep)
		if dep == "" || strings.HasPrefix(dep, "task:") {
			return nil, fmt.Errorf("invalid dependency id")
		}
		expectedDeps = append(expectedDeps, Address(KindCanonicalState, author, "task:"+dep))
	}
	if !sameStrings(TagAll(ev, "depends-on"), expectedDeps) {
		return nil, fmt.Errorf("dependency tags do not match content")
	}
	if state.Metadata == nil {
		state.Metadata = map[string]string{}
	}
	repoTag := markedTag(ev, "a", "nip34-repo")
	if strings.TrimSpace(state.Metadata["nip34.repo_addr"]) != repoTag {
		return nil, fmt.Errorf("repository tag does not match content")
	}
	rootTag := markedTag(ev, "e", "nip34-root")
	if strings.TrimSpace(state.Metadata["nostr.id"]) != rootTag {
		return nil, fmt.Errorf("NIP-34 root tag does not match content")
	}
	issue := &beadspb.Issue{Id: state.ID, Title: state.Title, Description: state.Description, Status: ParseStatus(state.Status), Priority: ParsePriority(state.Priority), Epic: state.Epic, Assignee: state.Assignee, Labels: append([]string(nil), state.Labels...), DependsOn: append([]string(nil), state.DependsOn...), Metadata: &beadspb.Metadata{Custom: state.Metadata}}
	if state.Created != "" {
		created, err := time.Parse(time.RFC3339, state.Created)
		if err != nil {
			return nil, fmt.Errorf("invalid created timestamp")
		}
		issue.Created = timestamppb.New(created.UTC())
	}
	if state.Updated != "" {
		updated, err := time.Parse(time.RFC3339, state.Updated)
		if err != nil {
			return nil, fmt.Errorf("invalid updated timestamp")
		}
		issue.Updated = timestamppb.New(updated.UTC())
	}
	return issue, nil
}

func exactlyOneTag(ev *gonostr.Event, name string) (string, error) {
	values := TagAll(ev, name)
	if len(values) != 1 || strings.TrimSpace(values[0]) == "" {
		return "", fmt.Errorf("expected exactly one %s tag", name)
	}
	return values[0], nil
}

func requireOptionalTagAgreement(ev *gonostr.Event, name, expected string) error {
	values := TagAll(ev, name)
	if expected == "" {
		if len(values) != 0 {
			return fmt.Errorf("%s tag does not match content", name)
		}
		return nil
	}
	if len(values) != 1 || values[0] != expected {
		return fmt.Errorf("%s tag does not match content", name)
	}
	return nil
}

func validTaskStatus(status string) bool {
	switch status {
	case "open", "in_progress", "blocked", "closed":
		return true
	default:
		return false
	}
}

func sameStrings(a, b []string) bool {
	a, b = append([]string(nil), a...), append([]string(nil), b...)
	for _, values := range [][]string{a, b} {
		for i := range values {
			values[i] = strings.TrimSpace(values[i])
		}
		sort.Strings(values)
		for i, value := range values {
			if value == "" || (i > 0 && value == values[i-1]) {
				return false
			}
		}
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hasExactTag(ev *gonostr.Event, name, value string) bool {
	for _, tag := range ev.Tags {
		if len(tag) >= 2 && tag[0] == name && tag[1] == value {
			return true
		}
	}
	return false
}

func markedTag(ev *gonostr.Event, name, marker string) string {
	var value string
	for _, tag := range ev.Tags {
		if len(tag) >= 4 && tag[0] == name && tag[3] == marker {
			if value != "" {
				return "__duplicate__"
			}
			value = tag[1]
		}
	}
	return value
}

func ensureDecoderEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("task state contains trailing JSON")
	}
	return nil
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
	return BuildClaimDispatchAtRevision(taskID, claimer, "", recipient, now)
}

func BuildClaimDispatchAtRevision(taskID, claimer, baseEventID, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("task/claim", recipient, map[string]string{"task_id": taskID, "claimer": claimer, "base_event_id": baseEventID, "dispatch": "fleet-worker"}, now)
}

func BuildAssignCommand(taskID, assignee, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildAssignCommandAtRevision(taskID, assignee, "", recipient, now)
}

func BuildAssignCommandAtRevision(taskID, assignee, baseEventID, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("task/assign", recipient, map[string]string{"task_id": taskID, "assignee": assignee, "base_event_id": baseEventID}, now)
}

func BuildCloseCommand(taskID, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildCloseCommandAtRevision(taskID, "", recipient, now)
}

func BuildCloseCommandAtRevision(taskID, baseEventID, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("task/close", recipient, map[string]string{"task_id": taskID, "base_event_id": baseEventID}, now)
}

func BuildQueueEnqueueCommand(queue, taskID, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildQueueEnqueueCommandForRepo("", queue, taskID, recipient, now)
}

func BuildQueueEnqueueCommandForRepo(repoAddr, queue, taskID, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildQueueEnqueueCommandAtRevision(repoAddr, queue, taskID, "", recipient, now)
}

func BuildQueueEnqueueCommandAtRevision(repoAddr, queue, taskID, baseEventID, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("queue/enqueue", recipient, map[string]string{"repo_addr": repoAddr, "queue": queue, "task_id": taskID, "base_event_id": baseEventID}, now)
}

func BuildQueueDequeueCommand(queue, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildQueueDequeueCommandForRepo("", queue, recipient, now)
}

func BuildQueueDequeueCommandForRepo(repoAddr, queue, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildQueueDequeueCommandAtRevision(repoAddr, queue, "", recipient, now)
}

func BuildQueueDequeueCommandAtRevision(repoAddr, queue, baseEventID, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("queue/dequeue", recipient, map[string]string{"repo_addr": repoAddr, "queue": queue, "base_event_id": baseEventID}, now)
}

func BuildQueueListCommand(queue, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildQueueListCommandForRepo("", queue, recipient, now)
}

func BuildQueueListCommandForRepo(repoAddr, queue, recipient string, now time.Time) (*gonostr.Event, error) {
	return BuildContextVMCommand("queue/list", recipient, map[string]string{"repo_addr": repoAddr, "queue": queue}, now)
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
