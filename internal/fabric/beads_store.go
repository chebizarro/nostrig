package fabric

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const rawBeadsRecord = "nostrig.bd.raw.v1"

// BeadsStore uses bd's supported JSONL export/import API. Unknown fields are
// retained in Metadata.Custom as the original JSON record, making a relay
// round-trip lossless even when the protobuf has not gained a typed field yet.
type BeadsStore struct {
	Directory string
	Binary    string
	Actor     string
	metadata  *JSONStore
}

func (s *BeadsStore) command(ctx context.Context, args ...string) *exec.Cmd {
	bin := s.Binary
	if bin == "" {
		bin = "bd"
	}
	base := []string{"-C", s.Directory}
	if s.Actor != "" {
		base = append(base, "--actor", s.Actor)
	}
	return exec.CommandContext(ctx, bin, append(base, args...)...)
}

func (s *BeadsStore) validate() error {
	if strings.TrimSpace(s.Directory) == "" {
		return fmt.Errorf("Beads workspace directory is required")
	}
	return nil
}

func (s *BeadsStore) Load(ctx context.Context) (*beadspb.Export, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	out, err := s.command(ctx, "export").Output()
	if err != nil {
		return nil, fmt.Errorf("bd export: %w", err)
	}
	export := &beadspb.Export{}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 16*1024*1024)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("bd export JSONL: %w", err)
		}
		if rowString(row, "issue_type") == "epic" {
			export.Epics = append(export.Epics, rowToEpic(row, line))
		} else {
			export.Issues = append(export.Issues, rowToIssue(row, line))
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return export, nil
}

func (s *BeadsStore) Save(ctx context.Context, export *beadspb.Export) error {
	if err := s.validate(); err != nil {
		return err
	}
	var input bytes.Buffer
	encoder := json.NewEncoder(&input)
	for _, epic := range export.Epics {
		if epic == nil {
			continue
		}
		row := rawRow(epic.Metadata)
		row["_type"], row["issue_type"], row["id"], row["title"] = "issue", "epic", epic.Id, epic.Name
		row["description"], row["status"] = epic.Description, statusString(epic.Status)
		setTimes(row, epic.Created, epic.Updated)
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	for _, issue := range export.Issues {
		if issue == nil {
			continue
		}
		row := rawRow(issue.Metadata)
		row["_type"], row["id"], row["title"] = "issue", issue.Id, issue.Title
		if _, ok := row["issue_type"]; !ok {
			row["issue_type"] = "task"
		}
		row["description"], row["status"] = issue.Description, statusString(issue.Status)
		if issue.Priority != beadspb.Priority_PRIORITY_UNSPECIFIED {
			row["priority"] = priorityNumber(issue.Priority)
		}
		row["assignee"], row["labels"] = issue.Assignee, issue.Labels
		deps := retainedDependencies(row["dependencies"])
		for _, id := range issue.DependsOn {
			deps = append(deps, map[string]any{"issue_id": issue.Id, "depends_on_id": id, "type": "blocks"})
		}
		if issue.Epic != "" {
			deps = append(deps, map[string]any{"issue_id": issue.Id, "depends_on_id": issue.Epic, "type": "parent-child"})
		}
		row["dependencies"] = deps
		setTimes(row, issue.Created, issue.Updated)
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	cmd := s.command(ctx, "import", "-", "--json")
	cmd.Stdin = &input
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bd import: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func (s *BeadsStore) metadataStore() *JSONStore {
	if s.metadata == nil {
		s.metadata = &JSONStore{Path: strings.TrimRight(s.Directory, "/") + "/.beads/nostrig-fabric-metadata.json"}
	}
	return s.metadata
}
func (s *BeadsStore) Seen(ctx context.Context, id string) (bool, error) {
	return s.metadataStore().Seen(ctx, id)
}
func (s *BeadsStore) MarkSeen(ctx context.Context, id string) error {
	return s.metadataStore().MarkSeen(ctx, id)
}
func (s *BeadsStore) OutboundDigest(ctx context.Context) (string, error) {
	return s.metadataStore().OutboundDigest(ctx)
}
func (s *BeadsStore) SetOutboundDigest(ctx context.Context, value string) error {
	return s.metadataStore().SetOutboundDigest(ctx, value)
}

func rowToIssue(row map[string]any, raw []byte) *beadspb.Issue {
	issue := &beadspb.Issue{Id: rowString(row, "id"), Title: rowString(row, "title"), Description: rowString(row, "description"), Status: parseStatus(rowString(row, "status")), Assignee: rowString(row, "assignee"), Epic: rowString(row, "epic"), Metadata: rawMetadata(raw)}
	if p, ok := row["priority"].(float64); ok {
		issue.Priority = priorityFromNumber(int(p))
	}
	issue.Labels = stringSlice(row["labels"])
	if deps, ok := row["dependencies"].([]any); ok {
		for _, dep := range deps {
			if object, ok := dep.(map[string]any); ok {
				id, kind := rowString(object, "depends_on_id"), rowString(object, "type")
				if kind == "parent-child" {
					issue.Epic = id
				} else if id != "" && kind == "blocks" {
					issue.DependsOn = append(issue.DependsOn, id)
				}
			}
		}
	}
	issue.Created, issue.Updated = parseTime(rowString(row, "created_at")), parseTime(rowString(row, "updated_at"))
	return issue
}
func rowToEpic(row map[string]any, raw []byte) *beadspb.Epic {
	return &beadspb.Epic{Id: rowString(row, "id"), Name: rowString(row, "title"), Description: rowString(row, "description"), Status: parseStatus(rowString(row, "status")), Created: parseTime(rowString(row, "created_at")), Updated: parseTime(rowString(row, "updated_at")), Metadata: rawMetadata(raw)}
}
func rawMetadata(raw []byte) *beadspb.Metadata {
	return &beadspb.Metadata{Custom: map[string]string{rawBeadsRecord: string(raw)}}
}
func rawRow(meta *beadspb.Metadata) map[string]any {
	row := map[string]any{}
	if meta != nil {
		_ = json.Unmarshal([]byte(meta.Custom[rawBeadsRecord]), &row)
	}
	return row
}
func rowString(row map[string]any, key string) string {
	if value, ok := row[key].(string); ok {
		return value
	}
	return ""
}
func stringSlice(value any) []string {
	list, _ := value.([]any)
	out := make([]string, 0, len(list))
	for _, v := range list {
		if text, ok := v.(string); ok {
			out = append(out, text)
		}
	}
	return out
}
func retainedDependencies(value any) []map[string]any {
	list, _ := value.([]any)
	out := make([]map[string]any, 0, len(list))
	for _, item := range list {
		if dep, ok := item.(map[string]any); ok && rowString(dep, "type") != "blocks" && rowString(dep, "type") != "parent-child" {
			out = append(out, dep)
		}
	}
	return out
}
func parseTime(value string) *timestamppb.Timestamp {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil
	}
	return timestamppb.New(parsed)
}
func setTimes(row map[string]any, created, updated *timestamppb.Timestamp) {
	if created != nil {
		row["created_at"] = created.AsTime().Format(time.RFC3339Nano)
	}
	if updated != nil {
		row["updated_at"] = updated.AsTime().Format(time.RFC3339Nano)
	}
}
func statusString(value beadspb.Status) string {
	return strings.ToLower(strings.TrimPrefix(value.String(), "STATUS_"))
}
func priorityNumber(value beadspb.Priority) int {
	n, _ := strconv.Atoi(strings.TrimPrefix(value.String(), "PRIORITY_P"))
	return n
}
func priorityFromNumber(value int) beadspb.Priority {
	if value < 0 || value > 4 {
		return beadspb.Priority_PRIORITY_UNSPECIFIED
	}
	return beadspb.Priority(value + 1)
}
