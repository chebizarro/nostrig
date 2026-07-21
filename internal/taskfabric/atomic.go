package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/proto"
)

const conflictErrorCode = -32009

// TaskRecord is the current canonical task plus the event that established it.
type TaskRecord struct {
	Issue     *beadspb.Issue
	EventID   string
	CreatedAt time.Time
	event     *gonostr.Event
}

// QueueLease is a durable reservation of one queue item.
type QueueLease struct {
	TaskID    string    `json:"task_id"`
	Worker    string    `json:"worker"`
	LeaseID   string    `json:"lease_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// QueueRecord is the current canonical queue collection and its reservations.
type QueueRecord struct {
	TaskIDs   []string
	Leases    []QueueLease
	EventID   string
	CreatedAt time.Time
}

type TaskMutationResult struct {
	Issue     *beadspb.Issue
	Delete    bool
	Unchanged bool
}

type QueueMutationResult struct {
	Queue     *QueueRecord
	Unchanged bool
}

type TaskMutation func(current *TaskRecord) (TaskMutationResult, error)
type QueueMutation func(current *QueueRecord) (QueueMutationResult, error)

// Ledger exposes atomic resource mutations. Implementations must invoke the
// callback and publish/store its result while holding the resource lock.
type Ledger interface {
	GetTask(ctx context.Context, id string) (*beadspb.Issue, error)
	MutateTask(ctx context.Context, id string, mutate TaskMutation) (*TaskRecord, error)
	GetQueue(ctx context.Context, repoAddr, queue string) (*QueueRecord, error)
	MutateQueue(ctx context.Context, repoAddr, queue string, mutate QueueMutation) (*QueueRecord, error)
}

// ConflictError is serialized as a structured JSON-RPC conflict response.
type ConflictError struct {
	Resource        string `json:"resource"`
	Reason          string `json:"reason"`
	ExpectedEventID string `json:"expected_event_id,omitempty"`
	ActualEventID   string `json:"actual_event_id,omitempty"`
	Assignee        string `json:"assignee,omitempty"`
	Status          string `json:"status,omitempty"`
}

func (e *ConflictError) Error() string {
	if e == nil {
		return "conflict"
	}
	return fmt.Sprintf("%s conflict: %s", e.Resource, e.Reason)
}

func (e *ConflictError) responseData() json.RawMessage {
	data, _ := json.Marshal(e)
	return data
}

func requireBaseParam(params map[string]any) (string, error) {
	raw, ok := params["base_event_id"]
	if !ok {
		return "", fmt.Errorf("base_event_id is required")
	}
	return strings.TrimSpace(fmt.Sprint(raw)), nil
}

func checkTaskBase(current *TaskRecord, expected string) error {
	actual := ""
	if current != nil {
		actual = current.EventID
	}
	if expected != actual {
		err := &ConflictError{Resource: "task", Reason: "stale_revision", ExpectedEventID: expected, ActualEventID: actual}
		if current != nil && current.Issue != nil {
			err.Assignee = current.Issue.Assignee
			err.Status = statusString(current.Issue)
		}
		return err
	}
	return nil
}

func checkQueueBase(current *QueueRecord, expected string) error {
	actual := ""
	if current != nil {
		actual = current.EventID
	}
	if expected != actual {
		return &ConflictError{Resource: "queue", Reason: "stale_revision", ExpectedEventID: expected, ActualEventID: actual}
	}
	return nil
}

func cloneIssue(issue *beadspb.Issue) *beadspb.Issue {
	if issue == nil {
		return nil
	}
	return proto.Clone(issue).(*beadspb.Issue)
}

func cloneTaskRecord(record *TaskRecord) *TaskRecord {
	if record == nil {
		return nil
	}
	out := *record
	out.Issue = cloneIssue(record.Issue)
	if record.event != nil {
		copied := *record.event
		out.event = &copied
	}
	return &out
}

func cloneQueueRecord(record *QueueRecord) *QueueRecord {
	if record == nil {
		return nil
	}
	out := *record
	out.TaskIDs = append([]string(nil), record.TaskIDs...)
	out.Leases = append([]QueueLease(nil), record.Leases...)
	return &out
}

func activeQueueLeases(record *QueueRecord, now time.Time) []QueueLease {
	if record == nil {
		return nil
	}
	out := make([]QueueLease, 0, len(record.Leases))
	for _, lease := range record.Leases {
		if lease.TaskID != "" && lease.Worker != "" && lease.LeaseID != "" && lease.ExpiresAt.After(now) {
			out = append(out, lease)
		}
	}
	return out
}

func availableQueueIDs(record *QueueRecord, now time.Time) []string {
	if record == nil {
		return nil
	}
	leased := make(map[string]struct{}, len(record.Leases))
	for _, lease := range activeQueueLeases(record, now) {
		leased[lease.TaskID] = struct{}{}
	}
	out := make([]string, 0, len(record.TaskIDs))
	for _, id := range record.TaskIDs {
		if _, ok := leased[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

func statusString(issue *beadspb.Issue) string {
	if issue == nil {
		return ""
	}
	switch issue.Status {
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

func eventID(ev *gonostr.Event) string {
	if ev == nil {
		return ""
	}
	return ev.ID.Hex()
}
