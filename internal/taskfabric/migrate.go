package taskfabric

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MigrateOptions struct {
	OutDir    string
	Relays    []string
	Signer    nip34.Signer
	Publisher EventPublisher
	DryRun    bool
}

type MigrateResult struct {
	Export         *beadspb.Export
	Events         []*gonostr.Event
	PublishedCount int
}

func LoadLocalExport(outDir string) (*beadspb.Export, error) {
	issues, err := LoadLocalIssues(outDir)
	if err != nil {
		return nil, err
	}
	epics, err := loadLocalEpics(outDir)
	if err != nil {
		return nil, err
	}
	export := &beadspb.Export{Epics: epics}
	for _, snap := range issues {
		if snap != nil {
			export.Issues = append(export.Issues, snap.ToIssue())
		}
	}
	sort.Slice(export.Issues, func(i, j int) bool { return export.Issues[i].Id < export.Issues[j].Id })
	return export, nil
}

func Migrate(ctx context.Context, opts MigrateOptions) (*MigrateResult, error) {
	export, err := LoadLocalExport(opts.OutDir)
	if err != nil {
		return nil, err
	}
	events, err := nip34.BuildCanonicalEvents(export, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	result := &MigrateResult{Export: export, Events: events}
	if opts.DryRun || len(events) == 0 {
		return result, nil
	}
	if opts.Signer == nil {
		return nil, fmt.Errorf("migrate requires signer")
	}
	publisher := opts.Publisher
	if publisher == nil {
		publisher = nip34.NewPublisher()
	}
	relays := cleanStrings(opts.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("migrate requires at least one relay")
	}
	if err := publisher.Publish(ctx, relays, opts.Signer, events); err != nil {
		return nil, err
	}
	result.PublishedCount = len(events)
	return result, nil
}

func loadLocalEpics(outDir string) ([]*beadspb.Epic, error) {
	path := filepath.Join(strings.TrimSpace(outDir), ".beads", "epics.jsonl")
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []*beadspb.Epic
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return nil, fmt.Errorf("decode local epic %s: %w", path, err)
		}
		epic := epicFromRaw(raw)
		if strings.TrimSpace(epic.Id) != "" {
			out = append(out, epic)
		}
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Id < out[j].Id })
	return out, nil
}

func epicFromRaw(raw map[string]json.RawMessage) *beadspb.Epic {
	epic := &beadspb.Epic{Id: rawString(raw, "id"), Name: rawString(raw, "name"), Description: rawString(raw, "description"), Status: nip34.ParseStatus(rawString(raw, "status")), Metadata: &beadspb.Metadata{Custom: map[string]string{}}}
	if epic.Name == "" {
		epic.Name = epic.Id
	}
	if created, err := parseTime(firstRawString(raw, "created", "created_at")); err == nil && !created.IsZero() {
		epic.Created = timestamppb.New(created)
	}
	if updated, err := parseTime(firstRawString(raw, "updated", "updated_at")); err == nil && !updated.IsZero() {
		epic.Updated = timestamppb.New(updated)
	}
	for k, v := range rawMap(raw, "metadata") {
		epic.Metadata.Custom[k] = fmt.Sprint(v)
	}
	return epic
}
