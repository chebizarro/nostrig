package beads

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/chebizarro/nostrig/gen/beads"
	"github.com/chebizarro/nostrig/internal/taskmodel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Renderer handles rendering protobuf beads to JSONL files (jira-beads-sync compatible).
type Renderer struct {
	outputDir string
}

// NewRenderer creates a new JSONL renderer.
func NewRenderer(outputDir string) *Renderer {
	return &Renderer{
		outputDir: outputDir,
	}
}

// RenderExport renders a beads export to JSONL files.
func (r *Renderer) RenderExport(export *pb.Export) error {
	if export == nil {
		return fmt.Errorf("export is nil")
	}

	if err := r.ensureDirectory(); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	issuesFile := filepath.Join(r.outputDir, ".beads", "issues.jsonl")
	if err := r.renderIssuesToJSONL(issuesFile, export.Issues); err != nil {
		return fmt.Errorf("failed to render issues: %w", err)
	}

	if len(export.Epics) > 0 {
		epicsFile := filepath.Join(r.outputDir, ".beads", "epics.jsonl")
		if err := r.renderEpicsToJSONL(epicsFile, export.Epics); err != nil {
			return fmt.Errorf("failed to render epics: %w", err)
		}
	}

	return nil
}

func (r *Renderer) ensureDirectory() error {
	beadsDir := filepath.Join(r.outputDir, ".beads")
	return os.MkdirAll(beadsDir, 0755)
}

// BeadsIssue is the complete Beads v1.0.3 issue document.
type BeadsIssue = taskmodel.IssueDocument

// BeadsEpic represents a beads epic in JSON format (jira-beads-sync compatible).
type BeadsEpic struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Status      string            `json:"status"`
	Created     string            `json:"created,omitempty"`
	Updated     string            `json:"updated,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (r *Renderer) renderIssuesToJSONL(filename string, issues []*pb.Issue) (err error) {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	encoder := json.NewEncoder(file)
	for _, issue := range issues {
		if issue == nil {
			continue
		}
		jsonIssue, err := r.issueToJSON(issue)
		if err != nil {
			return fmt.Errorf("failed to convert issue %s: %w", issue.Id, err)
		}
		if err := encoder.Encode(jsonIssue); err != nil {
			return fmt.Errorf("failed to encode issue %s: %w", issue.Id, err)
		}
	}

	return nil
}

func (r *Renderer) renderEpicsToJSONL(filename string, epics []*pb.Epic) (err error) {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	encoder := json.NewEncoder(file)
	for _, epic := range epics {
		if epic == nil {
			continue
		}
		jsonEpic := r.epicToJSON(epic)
		if err := encoder.Encode(jsonEpic); err != nil {
			return fmt.Errorf("failed to encode epic %s: %w", epic.Id, err)
		}
	}

	return nil
}

func (r *Renderer) issueToJSON(issue *pb.Issue) (*BeadsIssue, error) {
	doc, err := taskmodel.FromProto(issue)
	if err != nil {
		return nil, err
	}
	doc.SchemaVersion = ""
	doc.RecordType = "issue"
	doc.DependsOn = nil
	return doc, nil
}

func (r *Renderer) epicToJSON(epic *pb.Epic) *BeadsEpic {
	jsonEpic := &BeadsEpic{
		ID:          epic.Id,
		Name:        epic.Name,
		Description: epic.Description,
		Status:      r.statusToString(epic.Status),
	}

	if epic.Created != nil {
		jsonEpic.Created = r.timestampToString(epic.Created)
	}
	if epic.Updated != nil {
		jsonEpic.Updated = r.timestampToString(epic.Updated)
	}

	if epic.Metadata != nil {
		jsonEpic.Metadata = make(map[string]string)

		if epic.Metadata.JiraKey != "" {
			jsonEpic.Metadata["jiraKey"] = epic.Metadata.JiraKey
		}
		if epic.Metadata.JiraId != "" {
			jsonEpic.Metadata["jiraId"] = epic.Metadata.JiraId
		}
		if epic.Metadata.JiraIssueType != "" {
			jsonEpic.Metadata["jiraIssueType"] = epic.Metadata.JiraIssueType
		}

		for k, v := range epic.Metadata.Custom {
			jsonEpic.Metadata[k] = v
		}
	}

	return jsonEpic
}

func (r *Renderer) statusToString(status pb.Status) string {
	switch status {
	case pb.Status_STATUS_OPEN:
		return "open"
	case pb.Status_STATUS_IN_PROGRESS:
		return "in_progress"
	case pb.Status_STATUS_BLOCKED:
		return "blocked"
	case pb.Status_STATUS_CLOSED:
		return "closed"
	case pb.Status_STATUS_DEFERRED:
		return "deferred"
	default:
		return "open"
	}
}

func (r *Renderer) priorityToString(priority pb.Priority) string {
	switch priority {
	case pb.Priority_PRIORITY_P0:
		return "p0"
	case pb.Priority_PRIORITY_P1:
		return "p1"
	case pb.Priority_PRIORITY_P2:
		return "p2"
	case pb.Priority_PRIORITY_P3:
		return "p3"
	case pb.Priority_PRIORITY_P4:
		return "p4"
	case pb.Priority_PRIORITY_P9:
		return "p9"
	default:
		return ""
	}
}

func (r *Renderer) timestampToString(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().Format("2006-01-02T15:04:05Z07:00")
}
