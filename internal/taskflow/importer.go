package taskflow

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"github.com/chebizarro/nostrig/internal/taskmodel"
)

type EventPublisher interface {
	Publish(context.Context, []string, nip34.Signer, []*gonostr.Event) error
}

type Options struct {
	Source          string
	StatePath       string
	CanonicalAuthor string
	Relays          []string
	Signer          nip34.Signer
	Publisher       EventPublisher
	DryRun          bool
	Now             time.Time
}

type Report struct {
	Source          string   `json:"source"`
	Mode            string   `json:"mode"`
	ProjectsRead    int      `json:"projects_read"`
	TasksRead       int      `json:"tasks_read"`
	EventsGenerated int      `json:"events_generated"`
	RecordsSkipped  int      `json:"records_skipped"`
	Published       int      `json:"published"`
	StatePath       string   `json:"state_path,omitempty"`
	Warnings        []string `json:"warnings,omitempty"`
	Authority       string   `json:"authority"`
}

type stateFile struct {
	Records map[string]string `json:"records"`
}

type project struct {
	ID, Name, Description, Status string
}

type parsedTask struct {
	Project, Title, Description, Status, Priority string
	Notes, Checkpoint                             []string
	Source                                        string
	Ordinal                                       int
}

var (
	checkboxRE = regexp.MustCompile(`^\s*[-*]\s*\[([ xX])\]\s*(.+?)\s*$`)
	priorityRE = regexp.MustCompile(`(?i)(?:^|[\s[(])P(?:riority)?\s*[:=-]?\s*([0-4]|9)(?:[\s\])]|$)`)
)

func Run(ctx context.Context, opts Options) (*Report, error) {
	source, err := filepath.Abs(strings.TrimSpace(opts.Source))
	if err != nil {
		return nil, err
	}
	projects, tasks, warnings, err := ParseDirectory(source)
	if err != nil {
		return nil, err
	}
	statePath := strings.TrimSpace(opts.StatePath)
	if statePath == "" {
		statePath = filepath.Join(source, ".nostrig", "taskflow-import-state.json")
	}
	state, err := loadState(statePath)
	if err != nil {
		return nil, err
	}
	export := &beadspb.Export{}
	nextState := stateFile{Records: cloneMap(state.Records)}
	for _, p := range projects {
		epic := &beadspb.Epic{Id: p.ID, Name: p.Name, Description: p.Description, Status: nip34.ParseStatus(p.Status), Metadata: &beadspb.Metadata{Custom: map[string]string{"taskflow.source": "PROJECTS.md"}}}
		hash, err := recordHash(epic)
		if err != nil {
			return nil, err
		}
		key := "epic:" + p.ID
		if state.Records[key] == hash {
			continue
		}
		export.Epics = append(export.Epics, epic)
		nextState.Records[key] = hash
	}
	for _, item := range tasks {
		doc := taskDocument(item)
		issue, err := taskmodel.ToProto(doc)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", item.Source, err)
		}
		hash, err := recordHash(doc)
		if err != nil {
			return nil, err
		}
		key := "task:" + issue.Id
		if state.Records[key] == hash {
			continue
		}
		export.Issues = append(export.Issues, issue)
		nextState.Records[key] = hash
	}
	skipped := len(projects) + len(tasks) - len(export.Epics) - len(export.Issues)
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	events, err := nip34.BuildCanonicalEvents(export, opts.CanonicalAuthor, now)
	if err != nil {
		return nil, err
	}
	report := &Report{
		Source: source, ProjectsRead: len(projects), TasksRead: len(tasks),
		EventsGenerated: len(events), RecordsSkipped: skipped, StatePath: statePath,
		Warnings: warnings, Authority: "Nostrig is authoritative after migration; TaskFlow is read-only/retired.",
	}
	if opts.DryRun {
		report.Mode = "dry-run"
		return report, nil
	}
	report.Mode = "publish"
	if len(events) != 0 {
		if opts.Signer == nil {
			return nil, fmt.Errorf("taskflow import requires signer")
		}
		if len(clean(opts.Relays)) == 0 {
			return nil, fmt.Errorf("taskflow import requires at least one relay")
		}
		publisher := opts.Publisher
		if publisher == nil {
			publisher = nip34.NewPublisher()
		}
		if err := publisher.Publish(ctx, clean(opts.Relays), opts.Signer, events); err != nil {
			return nil, err
		}
		report.Published = len(events)
	}
	if err := writeState(statePath, nextState); err != nil {
		return nil, err
	}
	return report, nil
}

func ParseDirectory(root string) ([]project, []parsedTask, []string, error) {
	projectsPath := filepath.Join(root, "PROJECTS.md")
	projects, warnings, err := parseProjects(projectsPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, nil, err
	}
	if os.IsNotExist(err) {
		warnings = append(warnings, "PROJECTS.md not found")
	}
	files, err := filepath.Glob(filepath.Join(root, "tasks", "*-tasks.md"))
	if err != nil {
		return nil, nil, nil, err
	}
	sort.Strings(files)
	var tasks []parsedTask
	for _, path := range files {
		items, fileWarnings, err := parseTasks(path, root)
		if err != nil {
			return nil, nil, nil, err
		}
		tasks = append(tasks, items...)
		warnings = append(warnings, fileWarnings...)
	}
	if len(files) == 0 {
		warnings = append(warnings, "no tasks/*-tasks.md files found")
	}
	return projects, tasks, warnings, nil
}

func parseProjects(path string) ([]project, []string, error) {
	rows, err := readMarkdownTable(path)
	if err != nil {
		return nil, nil, err
	}
	var out []project
	for _, row := range rows {
		name := first(row, "project", "name")
		if name == "" {
			continue
		}
		id := "taskflow-project-" + slug(name)
		out = append(out, project{ID: id, Name: name, Description: first(row, "notes", "description"), Status: mapStatus(first(row, "status"), false)})
	}
	return out, nil, nil
}

func parseTasks(path, root string) ([]parsedTask, []string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	rel, _ := filepath.Rel(root, path)
	projectName := strings.TrimSuffix(filepath.Base(path), "-tasks.md")
	rows := markdownTables(string(data))
	var out []parsedTask
	ordinal := 0
	for _, row := range rows {
		title := first(row, "task", "title", "item")
		if title == "" {
			continue
		}
		ordinal++
		out = append(out, parsedTask{Project: projectName, Title: title, Description: first(row, "description"), Status: mapStatus(first(row, "status"), false), Priority: mapPriority(first(row, "priority")), Notes: splitNotes(first(row, "notes", "note")), Checkpoint: splitNotes(first(row, "checkpoint", "checkpoints")), Source: filepath.ToSlash(rel), Ordinal: ordinal})
	}
	if len(out) > 0 {
		return out, nil, nil
	}
	var section string
	var current *parsedTask
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			section = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "#"))
			continue
		}
		if match := checkboxRE.FindStringSubmatch(line); match != nil {
			ordinal++
			title := strings.TrimSpace(match[2])
			priority := ""
			if p := priorityRE.FindStringSubmatch(title); p != nil {
				priority = "P" + p[1]
				title = strings.TrimSpace(priorityRE.ReplaceAllString(title, " "))
			}
			status := mapStatus(section, strings.EqualFold(match[1], "x"))
			out = append(out, parsedTask{Project: projectName, Title: title, Status: status, Priority: priority, Source: filepath.ToSlash(rel), Ordinal: ordinal})
			current = &out[len(out)-1]
			continue
		}
		if current == nil {
			continue
		}
		key, value, ok := metadataLine(line)
		if !ok {
			continue
		}
		switch key {
		case "status":
			current.Status = mapStatus(value, false)
		case "priority":
			current.Priority = mapPriority(value)
		case "description":
			current.Description = value
		case "note", "notes":
			current.Notes = append(current.Notes, value)
		case "checkpoint", "checkpoints":
			current.Checkpoint = append(current.Checkpoint, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	return out, nil, nil
}

func taskDocument(item parsedTask) *taskmodel.IssueDocument {
	id := stableID(item.Source, item.Title, item.Ordinal)
	projectID := "taskflow-project-" + slug(item.Project)
	doc := &taskmodel.IssueDocument{
		ID: id, Title: item.Title, Description: item.Description, Status: item.Status,
		Priority: item.Priority, IssueType: "task", Project: item.Project, Epic: projectID,
		Labels:   []string{"taskflow-import"},
		Metadata: map[string]string{"taskflow.source": item.Source, "taskflow.ordinal": fmt.Sprint(item.Ordinal)},
	}
	for i, note := range item.Notes {
		doc.Comments = append(doc.Comments, taskmodel.CommentDocument{ID: stableChildID("comment", id, i, note), IssueID: id, Author: "TaskFlow", Text: note})
	}
	for i, checkpoint := range item.Checkpoint {
		doc.Checkpoints = append(doc.Checkpoints, taskmodel.CheckpointDocument{ID: stableChildID("checkpoint", id, i, checkpoint), Actor: "TaskFlow", Status: item.Status, Summary: checkpoint})
	}
	return doc
}

func readMarkdownTable(path string) ([]map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return markdownTables(string(data)), nil
}

func markdownTables(data string) []map[string]string {
	lines := strings.Split(data, "\n")
	var out []map[string]string
	for i := 0; i+1 < len(lines); i++ {
		headers := cells(lines[i])
		if len(headers) < 2 || !separatorRow(lines[i+1]) {
			continue
		}
		i += 2
		for ; i < len(lines); i++ {
			values := cells(lines[i])
			if len(values) < 2 {
				i--
				break
			}
			row := map[string]string{}
			for j, header := range headers {
				if j < len(values) {
					row[strings.ToLower(strings.TrimSpace(header))] = strings.TrimSpace(values[j])
				}
			}
			out = append(out, row)
		}
	}
	return out
}

func cells(line string) []string {
	line = strings.TrimSpace(line)
	if !strings.Contains(line, "|") {
		return nil
	}
	line = strings.Trim(line, "|")
	parts := strings.Split(line, "|")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func separatorRow(line string) bool {
	parts := cells(line)
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.Trim(part, ":-") != "" || !strings.Contains(part, "-") {
			return false
		}
	}
	return true
}

func metadataLine(line string) (string, string, bool) {
	line = strings.TrimSpace(line)
	line = strings.TrimSpace(strings.TrimLeft(line, "-*"))
	key, value, ok := strings.Cut(line, ":")
	if !ok {
		return "", "", false
	}
	key = strings.ToLower(strings.Trim(strings.TrimSpace(key), "*_"))
	switch key {
	case "status", "priority", "description", "note", "notes", "checkpoint", "checkpoints":
		return key, strings.TrimSpace(value), strings.TrimSpace(value) != ""
	default:
		return "", "", false
	}
}

func mapStatus(raw string, checked bool) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	value = strings.NewReplacer("-", "_", " ", "_").Replace(value)
	switch value {
	case "done", "complete", "completed", "closed", "resolved":
		return "closed"
	case "doing", "active", "started", "in_progress", "wip":
		return "in_progress"
	case "blocked", "stuck", "waiting":
		return "blocked"
	case "deferred", "paused", "parked", "parking_lot", "icebox":
		return "deferred"
	case "open", "todo", "to_do", "backlog", "planned", "":
		if checked {
			return "closed"
		}
		return "open"
	default:
		if checked {
			return "closed"
		}
		return "open"
	}
}

func mapPriority(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	value = strings.Trim(value, "[]()")
	switch value {
	case "0", "p0", "critical", "urgent":
		return "P0"
	case "1", "p1", "high":
		return "P1"
	case "2", "p2", "medium", "normal":
		return "P2"
	case "3", "p3", "low":
		return "P3"
	case "4", "p4", "very_low", "very low":
		return "P4"
	case "9", "p9", "parking_lot", "parking lot", "unscheduled":
		return "P9"
	default:
		if match := priorityRE.FindStringSubmatch(value); match != nil {
			return "P" + match[1]
		}
		return ""
	}
}

func splitNotes(value string) []string {
	var out []string
	for _, note := range strings.Split(value, "<br>") {
		note = strings.TrimSpace(note)
		if note != "" && note != "-" {
			out = append(out, note)
		}
	}
	return out
}

func first(row map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(row[key]); value != "" && value != "-" {
			return value
		}
	}
	return ""
}

func slug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
			lastDash = false
		} else if !lastDash && b.Len() > 0 {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func stableID(source, title string, ordinal int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", filepath.ToSlash(source), strings.TrimSpace(title), ordinal)))
	return "taskflow-" + hex.EncodeToString(sum[:6])
}

func stableChildID(kind, taskID string, ordinal int, text string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%s", kind, taskID, ordinal, text)))
	return kind + "-" + hex.EncodeToString(sum[:6])
}

func recordHash(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func loadState(path string) (stateFile, error) {
	state := stateFile{Records: map[string]string{}}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode taskflow import state: %w", err)
	}
	if state.Records == nil {
		state.Records = map[string]string{}
	}
	return state, nil
}

func writeState(path string, state stateFile) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func clean(in []string) []string {
	var out []string
	seen := map[string]struct{}{}
	for _, value := range in {
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
