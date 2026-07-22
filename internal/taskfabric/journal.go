package taskfabric

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	gonostr "fiatjaf.com/nostr"
	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	"github.com/chebizarro/nostrig/internal/durable"
)

const commandJournalVersion = 1

const defaultCommandRetention = 30 * 24 * time.Hour

type CommandPhase string

const (
	CommandReceived        CommandPhase = "received"
	CommandResponsePending CommandPhase = "response_pending"
	CommandComplete        CommandPhase = "complete"
)

type CommandCursor struct {
	CreatedAt gonostr.Timestamp `json:"created_at"`
	EventID   string            `json:"event_id"`
}

func (c CommandCursor) Before(event *gonostr.Event) bool {
	if event == nil {
		return false
	}
	if event.CreatedAt != c.CreatedAt {
		return event.CreatedAt > c.CreatedAt
	}
	return event.ID.Hex() > c.EventID
}

type CommandRecord struct {
	EventID         string          `json:"event_id"`
	SourceEventID   string          `json:"source_event_id"`
	CommandEventIDs []string        `json:"command_event_ids,omitempty"`
	SourceEventIDs  []string        `json:"source_event_ids,omitempty"`
	RequestKey      string          `json:"request_key,omitempty"`
	RequestID       json.RawMessage `json:"request_id,omitempty"`
	Fingerprint     string          `json:"fingerprint"`
	Wrapped         bool            `json:"wrapped,omitempty"`
	Source          gonostr.Event   `json:"source"`
	Command         gonostr.Event   `json:"command"`
	Response        *gonostr.Event  `json:"response,omitempty"`
	ResponseJSON    string          `json:"response_json,omitempty"`
	Phase           CommandPhase    `json:"phase"`
	ReceivedAt      time.Time       `json:"received_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	CompletedAt     time.Time       `json:"completed_at,omitempty"`
}

type commandJournalDisk struct {
	Version      int              `json:"version"`
	RejectBefore time.Time        `json:"reject_before,omitempty"`
	Cursor       CommandCursor    `json:"cursor"`
	Commands     []*CommandRecord `json:"commands"`
}

type CommandJournal struct {
	mu        sync.Mutex
	file      durable.JSONFile[commandJournalDisk]
	retention time.Duration
}

type RequestIDConflictError struct {
	RequestID       string
	ExistingEventID string
	IncomingEventID string
}

func (e *RequestIDConflictError) Error() string {
	return fmt.Sprintf("json-rpc request id %s was reused with different command content", e.RequestID)
}

var ErrReplayExpired = errors.New("command is older than the durable replay window")

func OpenCommandJournal(path string, retention time.Duration) (*CommandJournal, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("command journal path is required")
	}
	if retention <= 0 {
		retention = defaultCommandRetention
	}
	j := &CommandJournal{
		retention: retention,
		file: durable.JSONFile[commandJournalDisk]{
			Path: path,
			New: func() commandJournalDisk {
				return commandJournalDisk{Version: commandJournalVersion, Commands: []*CommandRecord{}}
			},
		},
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	unlock, err := j.lockFile()
	if err != nil {
		return nil, err
	}
	defer unlock()
	disk, err := j.loadLocked()
	if err != nil {
		return nil, err
	}
	j.pruneLocked(&disk, time.Now().UTC())
	if err := j.file.Store(disk); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *CommandJournal) Begin(source, command *gonostr.Event, wrapped bool, now time.Time) (*CommandRecord, bool, error) {
	if source == nil || command == nil || source.ID == gonostr.ZeroID || command.ID == gonostr.ZeroID {
		return nil, false, fmt.Errorf("source and command event IDs are required")
	}
	now = now.UTC()
	requestID, requestKey, fingerprint, err := commandIdentity(command)
	if err != nil {
		return nil, false, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	unlock, err := j.lockFile()
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	disk, err := j.loadLocked()
	if err != nil {
		return nil, false, err
	}
	j.pruneLocked(&disk, now)
	commandTime := time.Unix(int64(command.CreatedAt), 0).UTC()
	if !disk.RejectBefore.IsZero() && !commandTime.After(disk.RejectBefore) {
		if err := j.file.Store(disk); err != nil {
			return nil, false, err
		}
		return nil, false, ErrReplayExpired
	}
	for _, existing := range disk.Commands {
		if existing.EventID == command.ID.Hex() || existing.SourceEventID == source.ID.Hex() ||
			contains(existing.CommandEventIDs, command.ID.Hex()) || contains(existing.SourceEventIDs, source.ID.Hex()) {
			if existing.Fingerprint != fingerprint {
				return nil, false, &RequestIDConflictError{RequestID: string(requestID), ExistingEventID: existing.EventID, IncomingEventID: command.ID.Hex()}
			}
			changed := appendProcessedEventIDs(existing, source.ID.Hex(), command.ID.Hex())
			if changed {
				existing.UpdatedAt = now
				if err := j.file.Store(disk); err != nil {
					return nil, false, err
				}
			}
			return cloneCommandRecord(existing), false, nil
		}
	}
	if requestKey != "" {
		for _, existing := range disk.Commands {
			if existing.RequestKey != requestKey {
				continue
			}
			if existing.Fingerprint != fingerprint {
				return nil, false, &RequestIDConflictError{RequestID: string(requestID), ExistingEventID: existing.EventID, IncomingEventID: command.ID.Hex()}
			}
			appendProcessedEventIDs(existing, source.ID.Hex(), command.ID.Hex())
			existing.UpdatedAt = now
			if err := j.file.Store(disk); err != nil {
				return nil, false, err
			}
			return cloneCommandRecord(existing), false, nil
		}
	}
	record := &CommandRecord{
		EventID:         command.ID.Hex(),
		SourceEventID:   source.ID.Hex(),
		CommandEventIDs: []string{command.ID.Hex()},
		SourceEventIDs:  []string{source.ID.Hex()},
		RequestKey:      requestKey,
		RequestID:       append(json.RawMessage(nil), requestID...),
		Fingerprint:     fingerprint,
		Wrapped:         wrapped,
		Source:          cloneNostrEvent(*source),
		Command:         cloneNostrEvent(*command),
		Phase:           CommandReceived,
		ReceivedAt:      now,
		UpdatedAt:       now,
	}
	disk.Commands = append(disk.Commands, record)
	if err := j.file.Store(disk); err != nil {
		return nil, false, err
	}
	return cloneCommandRecord(record), true, nil
}

func (j *CommandJournal) SaveResponse(eventID string, response *gonostr.Event, responseJSON string, now time.Time) (*CommandRecord, error) {
	if response == nil {
		return nil, fmt.Errorf("response event is required")
	}
	return j.update(eventID, now, func(record *CommandRecord) {
		copy := cloneNostrEvent(*response)
		record.Response = &copy
		record.ResponseJSON = responseJSON
		record.Phase = CommandResponsePending
	})
}

func (j *CommandJournal) Complete(eventID string, now time.Time) (*CommandRecord, error) {
	return j.update(eventID, now, func(record *CommandRecord) {
		record.Phase = CommandComplete
		record.CompletedAt = now.UTC()
	})
}

func (j *CommandJournal) Pending() ([]*CommandRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	unlock, err := j.lockFile()
	if err != nil {
		return nil, err
	}
	defer unlock()
	disk, err := j.loadLocked()
	if err != nil {
		return nil, err
	}
	out := make([]*CommandRecord, 0)
	for _, record := range disk.Commands {
		if record.Phase != CommandComplete {
			out = append(out, cloneCommandRecord(record))
		}
	}
	sort.Slice(out, func(i, k int) bool {
		if out[i].Source.CreatedAt != out[k].Source.CreatedAt {
			return out[i].Source.CreatedAt < out[k].Source.CreatedAt
		}
		return out[i].SourceEventID < out[k].SourceEventID
	})
	return out, nil
}

func (j *CommandJournal) Cursor() (CommandCursor, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	unlock, err := j.lockFile()
	if err != nil {
		return CommandCursor{}, err
	}
	defer unlock()
	disk, err := j.loadLocked()
	if err != nil {
		return CommandCursor{}, err
	}
	return disk.Cursor, nil
}

func (j *CommandJournal) AdvanceCursor(event *gonostr.Event) error {
	if event == nil || event.ID == gonostr.ZeroID {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	unlock, err := j.lockFile()
	if err != nil {
		return err
	}
	defer unlock()
	disk, err := j.loadLocked()
	if err != nil {
		return err
	}
	if disk.Cursor.Before(event) {
		disk.Cursor = CommandCursor{CreatedAt: event.CreatedAt, EventID: event.ID.Hex()}
		return j.file.Store(disk)
	}
	return nil
}

func (j *CommandJournal) update(eventID string, now time.Time, mutate func(*CommandRecord)) (*CommandRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	unlock, err := j.lockFile()
	if err != nil {
		return nil, err
	}
	defer unlock()
	disk, err := j.loadLocked()
	if err != nil {
		return nil, err
	}
	for _, record := range disk.Commands {
		if record.EventID != eventID {
			continue
		}
		mutate(record)
		record.UpdatedAt = now.UTC()
		if err := j.file.Store(disk); err != nil {
			return nil, err
		}
		return cloneCommandRecord(record), nil
	}
	return nil, fmt.Errorf("command %s not found", eventID)
}

func (j *CommandJournal) loadLocked() (commandJournalDisk, error) {
	disk, err := j.file.Load()
	if err != nil {
		return commandJournalDisk{}, err
	}
	if disk.Version == 0 {
		disk.Version = commandJournalVersion
	}
	if disk.Version != commandJournalVersion {
		return commandJournalDisk{}, fmt.Errorf("unsupported command journal version %d", disk.Version)
	}
	if disk.Commands == nil {
		disk.Commands = []*CommandRecord{}
	}
	return disk, nil
}

func (j *CommandJournal) pruneLocked(disk *commandJournalDisk, now time.Time) {
	cutoff := now.UTC().Add(-j.retention)
	if disk.RejectBefore.Before(cutoff) {
		disk.RejectBefore = cutoff
	}
	kept := disk.Commands[:0]
	for _, record := range disk.Commands {
		if record.Phase == CommandComplete && !record.CompletedAt.IsZero() && !record.CompletedAt.After(cutoff) {
			continue
		}
		kept = append(kept, record)
	}
	disk.Commands = kept
}

func (j *CommandJournal) lockFile() (func(), error) {
	dir := filepath.Dir(j.file.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create command journal lock directory: %w", err)
	}
	file, err := os.OpenFile(j.file.Path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open command journal lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock command journal: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	}, nil
}

func commandIdentity(command *gonostr.Event) (json.RawMessage, string, string, error) {
	var request cascontextvm.Request
	if err := json.Unmarshal([]byte(command.Content), &request); err != nil {
		return nil, "", "", fmt.Errorf("decode command identity: %w", err)
	}
	requestID := append(json.RawMessage(nil), request.ID...)
	requestKey := ""
	if len(requestID) > 0 && string(requestID) != "null" {
		requestKey = command.PubKey.Hex() + "|" + string(requestID)
	}
	sum := sha256.Sum256([]byte(command.PubKey.Hex() + "\x00" + command.Content))
	return requestID, requestKey, hex.EncodeToString(sum[:]), nil
}

func cloneCommandRecord(record *CommandRecord) *CommandRecord {
	if record == nil {
		return nil
	}
	out := *record
	out.RequestID = append(json.RawMessage(nil), record.RequestID...)
	out.CommandEventIDs = append([]string(nil), record.CommandEventIDs...)
	out.SourceEventIDs = append([]string(nil), record.SourceEventIDs...)
	out.Source = cloneNostrEvent(record.Source)
	out.Command = cloneNostrEvent(record.Command)
	if record.Response != nil {
		response := cloneNostrEvent(*record.Response)
		out.Response = &response
	}
	return &out
}

func appendProcessedEventIDs(record *CommandRecord, sourceID, commandID string) bool {
	changed := false
	if !contains(record.CommandEventIDs, commandID) {
		record.CommandEventIDs = append(record.CommandEventIDs, commandID)
		changed = true
	}
	if !contains(record.SourceEventIDs, sourceID) {
		record.SourceEventIDs = append(record.SourceEventIDs, sourceID)
		changed = true
	}
	return changed
}

func cloneNostrEvent(event gonostr.Event) gonostr.Event {
	event.Tags = append(gonostr.Tags(nil), event.Tags...)
	for i := range event.Tags {
		event.Tags[i] = append(gonostr.Tag(nil), event.Tags[i]...)
	}
	return event
}
