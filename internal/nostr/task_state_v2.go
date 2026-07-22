package nostr

import (
	"fmt"
	"sort"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"github.com/chebizarro/nostrig/internal/taskmodel"
)

type TaskStateSchemaVersion int

const (
	TaskStateVersionV1 TaskStateSchemaVersion = 1
	TaskStateVersionV2 TaskStateSchemaVersion = 2
)

// BuildTaskStateEvent writes the current canonical schema. The separate
// BuildTaskStateEventV1 helper exists only for migration tests and old peers.
func BuildTaskStateEvent(issue *beadspb.Issue, canonicalAuthor string, now time.Time) (*gonostr.Event, error) {
	canonicalAuthor, err := canonicalPubKey(canonicalAuthor)
	if err != nil {
		return nil, err
	}
	doc, err := taskmodel.FromProto(issue)
	if err != nil {
		return nil, err
	}
	content, err := taskmodel.EncodeCanonical(doc)
	if err != nil {
		return nil, err
	}
	tags := gonostr.Tags{
		{"d", "task:" + doc.ID},
		{"domain", "task"},
		{"schema", TaskStateSchemaV2},
		{"status", doc.Status},
	}
	if doc.Priority != "" {
		tags = append(tags, gonostr.Tag{"priority", doc.Priority})
	}
	if doc.Assignee != "" {
		tags = append(tags, gonostr.Tag{"assignee", doc.Assignee})
	}
	if doc.Epic != "" {
		tags = append(tags,
			gonostr.Tag{"a", Address(KindNamedList, canonicalAuthor, "epic:"+doc.Epic)},
			gonostr.Tag{"epic", doc.Epic},
		)
	}
	dependencyCoordinates := map[string]struct{}{}
	for _, dep := range doc.Dependencies {
		coordinate := Address(KindCanonicalState, canonicalAuthor, "task:"+dep.DependsOnID)
		if _, exists := dependencyCoordinates[coordinate]; !exists {
			dependencyCoordinates[coordinate] = struct{}{}
			tags = append(tags, gonostr.Tag{"depends-on", coordinate})
		}
		tags = append(tags, gonostr.Tag{"dependency", coordinate, dep.Type})
	}
	for _, label := range doc.Labels {
		tags = append(tags, gonostr.Tag{"t", label})
	}
	if value := doc.Metadata["nostr.id"]; value != "" {
		tags = append(tags, gonostr.Tag{"e", value, "", "nip34-root"})
	}
	if doc.Repository != "" {
		tags = append(tags, gonostr.Tag{"a", doc.Repository, "", "nip34-repo"})
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return &gonostr.Event{
		Kind:      gonostr.Kind(KindCanonicalState),
		CreatedAt: gonostr.Timestamp(now.Unix()),
		Tags:      tags,
		Content:   string(content),
	}, nil
}

// ParseTaskStateEvent accepts every supported schema and migrates v1 content
// into the complete in-memory model.
func ParseTaskStateEvent(ev *gonostr.Event) (*beadspb.Issue, error) {
	issue, _, err := ParseTaskStateEventVersioned(ev)
	return issue, err
}

func ParseTaskStateEventVersioned(ev *gonostr.Event) (*beadspb.Issue, TaskStateSchemaVersion, error) {
	if ev == nil {
		return nil, 0, fmt.Errorf("event is nil")
	}
	schema, err := exactlyOneTag(ev, "schema")
	if err != nil {
		return nil, 0, fmt.Errorf("unsupported task state schema")
	}
	switch schema {
	case TaskStateSchemaV1:
		legacy, err := parseTaskStateEventV1(ev)
		if err != nil {
			return nil, 0, err
		}
		migrated, err := MigrateTaskStateV1(legacy)
		return migrated, TaskStateVersionV1, err
	case TaskStateSchemaV2:
		issue, err := parseTaskStateEventV2(ev)
		return issue, TaskStateVersionV2, err
	default:
		return nil, 0, fmt.Errorf("unsupported task state schema %q", schema)
	}
}

// MigrateTaskStateV1 is deterministic and idempotent. It preserves all v1
// fields, promotes legacy depends_on entries to typed "blocks" dependencies,
// and mirrors the legacy repository metadata into the top-level repository.
func MigrateTaskStateV1(issue *beadspb.Issue) (*beadspb.Issue, error) {
	doc, err := taskmodel.FromProto(issue)
	if err != nil {
		return nil, err
	}
	return taskmodel.ToProto(doc)
}

func parseTaskStateEventV2(ev *gonostr.Event) (*beadspb.Issue, error) {
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
	if schema, err := exactlyOneTag(ev, "schema"); err != nil || schema != TaskStateSchemaV2 {
		return nil, fmt.Errorf("unsupported task state schema")
	}
	if domain, err := exactlyOneTag(ev, "domain"); err != nil || domain != "task" {
		return nil, fmt.Errorf("task state requires domain=task")
	}
	doc, err := taskmodel.DecodeCanonical([]byte(ev.Content))
	if err != nil {
		return nil, fmt.Errorf("decode task state: %w", err)
	}
	if doc.ID != strings.TrimPrefix(d, "task:") {
		return nil, fmt.Errorf("task content id does not match d tag")
	}
	if err := requireOptionalTagAgreement(ev, "status", doc.Status); err != nil {
		return nil, err
	}
	if err := requireOptionalTagAgreement(ev, "priority", doc.Priority); err != nil {
		return nil, err
	}
	if err := requireOptionalTagAgreement(ev, "assignee", doc.Assignee); err != nil {
		return nil, err
	}
	author := ev.PubKey.Hex()
	if doc.Epic == "" {
		if len(TagAll(ev, "epic")) != 0 {
			return nil, fmt.Errorf("epic tag does not match content")
		}
	} else {
		if err := requireOptionalTagAgreement(ev, "epic", doc.Epic); err != nil {
			return nil, err
		}
		if !hasExactTag(ev, "a", Address(KindNamedList, author, "epic:"+doc.Epic)) {
			return nil, fmt.Errorf("epic coordinate does not identify canonical author")
		}
	}
	if !sameStrings(TagAll(ev, "t"), doc.Labels) {
		return nil, fmt.Errorf("label tags do not match content")
	}
	expectedCoordinates := make([]string, 0, len(doc.Dependencies))
	expectedCoordinateSet := map[string]struct{}{}
	expectedTyped := make([]string, 0, len(doc.Dependencies))
	for _, dep := range doc.Dependencies {
		coordinate := Address(KindCanonicalState, author, "task:"+dep.DependsOnID)
		if _, exists := expectedCoordinateSet[coordinate]; !exists {
			expectedCoordinateSet[coordinate] = struct{}{}
			expectedCoordinates = append(expectedCoordinates, coordinate)
		}
		expectedTyped = append(expectedTyped, coordinate+"|"+dep.Type)
	}
	if !sameStrings(TagAll(ev, "depends-on"), expectedCoordinates) {
		return nil, fmt.Errorf("dependency tags do not match content")
	}
	actualTyped, err := dependencyTags(ev)
	if err != nil || !sameStrings(actualTyped, expectedTyped) {
		return nil, fmt.Errorf("typed dependency tags do not match content")
	}
	if repoTag := markedTag(ev, "a", "nip34-repo"); repoTag != doc.Repository {
		return nil, fmt.Errorf("repository tag does not match content")
	}
	if rootTag := markedTag(ev, "e", "nip34-root"); rootTag != strings.TrimSpace(doc.Metadata["nostr.id"]) {
		return nil, fmt.Errorf("NIP-34 root tag does not match content")
	}
	return taskmodel.ToProto(doc)
}

func dependencyTags(ev *gonostr.Event) ([]string, error) {
	var out []string
	for _, tag := range ev.Tags {
		if len(tag) == 0 || tag[0] != "dependency" {
			continue
		}
		if len(tag) != 3 || strings.TrimSpace(tag[1]) == "" || strings.TrimSpace(tag[2]) == "" {
			return nil, fmt.Errorf("malformed dependency tag")
		}
		out = append(out, tag[1]+"|"+tag[2])
	}
	sort.Strings(out)
	return out, nil
}
