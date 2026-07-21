// Package fabric maps the complete beads model to the fleet task-fabric
// representation: kind 30900 task state plus NIP-51 queue/epic collections.
package fabric

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	fn "github.com/chebizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
	"google.golang.org/protobuf/encoding/protojson"
)

const schema = "cascadia.task.v1"

type taskEnvelope struct {
	Schema string          `json:"schema"`
	Issue  json.RawMessage `json:"issue"`
}

type epicEnvelope struct {
	Schema string          `json:"schema"`
	Epic   json.RawMessage `json:"epic"`
}

// Encode creates unsigned canonical events. Signing is deliberately a separate
// boundary: callers must send these events to Signet and never load an nsec.
func Encode(export *beadspb.Export, pubkey string, at time.Time) ([]*gonostr.Event, error) {
	if export == nil {
		return nil, fmt.Errorf("export is nil")
	}
	if strings.TrimSpace(pubkey) == "" {
		return nil, fmt.Errorf("signer pubkey is required")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	marshal := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}
	events := make([]*gonostr.Event, 0, len(export.Issues)+len(export.Epics))
	for _, issue := range export.Issues {
		if issue == nil || strings.TrimSpace(issue.Id) == "" {
			return nil, fmt.Errorf("task missing beads id")
		}
		body, err := marshal.Marshal(issue)
		if err != nil {
			return nil, fmt.Errorf("marshal task %q: %w", issue.Id, err)
		}
		content, err := json.Marshal(taskEnvelope{Schema: schema, Issue: body})
		if err != nil {
			return nil, err
		}
		tags := gonostr.Tags{{"d", "task:" + issue.Id}, {"t", "task"}, {"schema", schema}}
		if issue.Epic != "" {
			tags = append(tags, gonostr.Tag{"a", fmt.Sprintf("%d:%s:epic:%s", fn.KindNIP51Set, pubkey, issue.Epic), "epic"})
		}
		for _, dep := range issue.DependsOn {
			tags = append(tags, gonostr.Tag{"a", fmt.Sprintf("%d:%s:task:%s", fn.KindTaskState, pubkey, dep), "depends_on"})
		}
		if issue.Assignee != "" {
			if validPubkey(issue.Assignee) {
				tags = append(tags, gonostr.Tag{"p", issue.Assignee, "assignee"})
			} else {
				// Beads assignees are commonly names (for example "Strand"),
				// which must not be emitted as an invalid Nostr p-tag pubkey.
				tags = append(tags, gonostr.Tag{"assignee", issue.Assignee})
			}
		}
		for _, label := range issue.Labels {
			tags = append(tags, gonostr.Tag{"t", label})
		}
		events = append(events, &gonostr.Event{PubKey: pubkey, CreatedAt: gonostr.Timestamp(at.Unix()), Kind: fn.KindTaskState, Tags: tags, Content: string(content)})
	}
	for _, epic := range export.Epics {
		if epic == nil || strings.TrimSpace(epic.Id) == "" {
			return nil, fmt.Errorf("epic missing beads id")
		}
		body, err := marshal.Marshal(epic)
		if err != nil {
			return nil, fmt.Errorf("marshal epic %q: %w", epic.Id, err)
		}
		content, err := json.Marshal(epicEnvelope{Schema: schema, Epic: body})
		if err != nil {
			return nil, err
		}
		tags := gonostr.Tags{{"d", "epic:" + epic.Id}, {"t", "queue"}, {"t", "epic"}, {"schema", schema}}
		for _, issue := range export.Issues {
			if issue != nil && issue.Epic == epic.Id {
				tags = append(tags, gonostr.Tag{"a", fmt.Sprintf("%d:%s:task:%s", fn.KindTaskState, pubkey, issue.Id)})
			}
		}
		events = append(events, &gonostr.Event{PubKey: pubkey, CreatedAt: gonostr.Timestamp(at.Unix()), Kind: fn.KindNIP51Set, Tags: tags, Content: string(content)})
	}
	return events, nil
}

// Decode reconstructs the full beads protobuf. Event tags are indexes and
// authorization hints; content is the lossless canonical record. For relay
// input use ProjectVerified; Decode intentionally accepts unsigned events so
// callers can round-trip Encode output before the Signet boundary.
func Decode(events []*gonostr.Event) (*beadspb.Export, error) {
	r := newUncheckedReducer()
	for _, ev := range events {
		if _, err := r.Apply(ev); err != nil {
			return nil, err
		}
	}
	current := make([]*gonostr.Event, 0, len(r.current))
	for _, ev := range r.current {
		current = append(current, ev)
	}
	return decodeCurrent(current)
}

func decodeCurrent(events []*gonostr.Event) (*beadspb.Export, error) {
	out := &beadspb.Export{}
	for _, ev := range events {
		if ev == nil {
			continue
		}
		issue, epic, recognized, err := decodeStateEvent(ev)
		if err != nil {
			return nil, err
		}
		if !recognized {
			continue
		}
		if issue != nil {
			out.Issues = append(out.Issues, issue)
		}
		if epic != nil {
			out.Epics = append(out.Epics, epic)
		}
	}
	sort.Slice(out.Issues, func(i, j int) bool { return out.Issues[i].Id < out.Issues[j].Id })
	sort.Slice(out.Epics, func(i, j int) bool { return out.Epics[i].Id < out.Epics[j].Id })
	return out, nil
}

func decodeStateEvent(ev *gonostr.Event) (*beadspb.Issue, *beadspb.Epic, bool, error) {
	if ev == nil {
		return nil, nil, false, nil
	}
	unmarshal := protojson.UnmarshalOptions{DiscardUnknown: false}
	switch ev.Kind {
	case fn.KindTaskState:
		d, ok := fn.TagD(ev)
		if !ok || !strings.HasPrefix(d, "task:") {
			return nil, nil, false, nil
		}
		var env taskEnvelope
		if err := json.Unmarshal([]byte(ev.Content), &env); err != nil {
			return nil, nil, true, fmt.Errorf("decode %s: %w", d, err)
		}
		if env.Schema != schema {
			return nil, nil, true, fmt.Errorf("decode %s: unsupported schema %q", d, env.Schema)
		}
		if len(env.Issue) == 0 || string(env.Issue) == "null" {
			return nil, nil, true, fmt.Errorf("decode %s: issue is required", d)
		}
		issue := new(beadspb.Issue)
		if err := unmarshal.Unmarshal(env.Issue, issue); err != nil {
			return nil, nil, true, fmt.Errorf("decode %s: %w", d, err)
		}
		if d != "task:"+issue.Id {
			return nil, nil, true, fmt.Errorf("task address/id mismatch: %q != %q", d, issue.Id)
		}
		return issue, nil, true, nil
	case fn.KindNIP51Set:
		d, ok := fn.TagD(ev)
		if !ok || !strings.HasPrefix(d, "epic:") {
			return nil, nil, false, nil
		}
		var env epicEnvelope
		if err := json.Unmarshal([]byte(ev.Content), &env); err != nil {
			return nil, nil, true, fmt.Errorf("decode %s: %w", d, err)
		}
		if env.Schema != schema {
			return nil, nil, true, fmt.Errorf("decode %s: unsupported schema %q", d, env.Schema)
		}
		if len(env.Epic) == 0 || string(env.Epic) == "null" {
			return nil, nil, true, fmt.Errorf("decode %s: epic is required", d)
		}
		epic := new(beadspb.Epic)
		if err := unmarshal.Unmarshal(env.Epic, epic); err != nil {
			return nil, nil, true, fmt.Errorf("decode %s: %w", d, err)
		}
		if d != "epic:"+epic.Id {
			return nil, nil, true, fmt.Errorf("epic address/id mismatch: %q != %q", d, epic.Id)
		}
		return nil, epic, true, nil
	default:
		return nil, nil, false, nil
	}
}
