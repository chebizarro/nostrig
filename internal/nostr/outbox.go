package nostr

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	gonostr "fiatjaf.com/nostr"
	"github.com/chebizarro/nostrig/internal/durable"
)

const outboxVersion = 1

type OutboxState string

const (
	OutboxPending    OutboxState = "pending"
	OutboxDeadLetter OutboxState = "dead_letter"
)

// RelayDelivery is the durable state for one event/relay pair.
type RelayDelivery struct {
	URL          string    `json:"url"`
	Required     bool      `json:"required"`
	Acknowledged bool      `json:"acknowledged"`
	Attempts     int       `json:"attempts"`
	LastAttempt  time.Time `json:"last_attempt,omitempty"`
	NextAttempt  time.Time `json:"next_attempt,omitempty"`
	LastError    string    `json:"last_error,omitempty"`
	DeadLetter   bool      `json:"dead_letter,omitempty"`
}

// OutboxEntry contains one signed Nostr event. Retrying the same signed event
// preserves its event ID, so relay recovery cannot create another logical mutation.
type OutboxEntry struct {
	ID            string          `json:"id"`
	Event         gonostr.Event   `json:"event"`
	Quorum        int             `json:"quorum"`
	QuorumReached bool            `json:"quorum_reached"`
	State         OutboxState     `json:"state"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	Deliveries    []RelayDelivery `json:"deliveries"`
}

type outboxDisk struct {
	Version int            `json:"version"`
	Entries []*OutboxEntry `json:"entries"`
}

// OutboxStore is a versioned durable spool. The containing state directory is
// intentionally not outbox-specific so a later command journal can live beside it.
type OutboxStore struct {
	mu   sync.Mutex
	file durable.JSONFile[outboxDisk]
}

func OpenOutbox(path string) (*OutboxStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("outbox path is required")
	}
	s := &OutboxStore{file: durable.JSONFile[outboxDisk]{Path: path, New: func() outboxDisk {
		return outboxDisk{Version: outboxVersion, Entries: []*OutboxEntry{}}
	}}}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lockFile()
	if err != nil {
		return nil, err
	}
	defer unlock()
	disk, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	if err := s.file.Store(disk); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *OutboxStore) loadLocked() (outboxDisk, error) {
	disk, err := s.file.Load()
	if err != nil {
		return outboxDisk{}, err
	}
	if disk.Version == 0 {
		disk.Version = outboxVersion
	}
	if disk.Version != outboxVersion {
		return outboxDisk{}, fmt.Errorf("unsupported outbox version %d", disk.Version)
	}
	if disk.Entries == nil {
		disk.Entries = []*OutboxEntry{}
	}
	return disk, nil
}

func (s *OutboxStore) lockFile() (func(), error) {
	dir := filepath.Dir(s.file.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create outbox lock directory: %w", err)
	}
	file, err := os.OpenFile(s.file.Path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open outbox lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock outbox: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	}, nil
}

// List returns a snapshot ordered by creation time and event ID.
func (s *OutboxStore) List() ([]*OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lockFile()
	if err != nil {
		return nil, err
	}
	defer unlock()
	disk, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	out := cloneEntries(disk.Entries)
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (s *OutboxStore) DeadLetters() ([]*OutboxEntry, error) {
	entries, err := s.List()
	if err != nil {
		return nil, err
	}
	out := entries[:0]
	for _, entry := range entries {
		if entry.State == OutboxDeadLetter {
			out = append(out, entry)
		}
	}
	return out, nil
}

// Retry returns dead-lettered deliveries to pending state. With no IDs, all
// dead letters are retried. The running serve process will drain them.
func (s *OutboxStore) Retry(ids ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lockFile()
	if err != nil {
		return 0, err
	}
	defer unlock()
	disk, err := s.loadLocked()
	if err != nil {
		return 0, err
	}
	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id = strings.TrimSpace(id); id != "" {
			wanted[id] = struct{}{}
		}
	}
	count := 0
	now := time.Now().UTC()
	for _, entry := range disk.Entries {
		if len(wanted) > 0 {
			if _, ok := wanted[entry.ID]; !ok {
				continue
			}
		}
		changed := false
		for i := range entry.Deliveries {
			delivery := &entry.Deliveries[i]
			if delivery.Acknowledged || !delivery.DeadLetter {
				continue
			}
			delivery.Attempts = 0
			delivery.DeadLetter = false
			delivery.NextAttempt = now
			delivery.LastError = ""
			changed = true
		}
		if changed {
			entry.State = OutboxPending
			entry.UpdatedAt = now
			count++
		}
	}
	if count > 0 {
		if err := s.file.Store(disk); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func (s *OutboxStore) enqueue(entry *OutboxEntry) (*OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lockFile()
	if err != nil {
		return nil, err
	}
	defer unlock()
	disk, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	for _, existing := range disk.Entries {
		if existing.ID == entry.ID {
			return cloneEntry(existing), nil
		}
	}
	disk.Entries = append(disk.Entries, cloneEntry(entry))
	if err := s.file.Store(disk); err != nil {
		return nil, err
	}
	return cloneEntry(entry), nil
}

func (s *OutboxStore) put(entry *OutboxEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lockFile()
	if err != nil {
		return err
	}
	defer unlock()
	disk, err := s.loadLocked()
	if err != nil {
		return err
	}
	for i, existing := range disk.Entries {
		if existing.ID == entry.ID {
			disk.Entries[i] = cloneEntry(entry)
			return s.file.Store(disk)
		}
	}
	return fmt.Errorf("outbox event %s not found", entry.ID)
}

func (s *OutboxStore) remove(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lockFile()
	if err != nil {
		return err
	}
	defer unlock()
	disk, err := s.loadLocked()
	if err != nil {
		return err
	}
	for i, entry := range disk.Entries {
		if entry.ID == id {
			disk.Entries = append(disk.Entries[:i], disk.Entries[i+1:]...)
			return s.file.Store(disk)
		}
	}
	return nil
}

func cloneEntries(entries []*OutboxEntry) []*OutboxEntry {
	out := make([]*OutboxEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, cloneEntry(entry))
	}
	return out
}

func cloneEntry(entry *OutboxEntry) *OutboxEntry {
	if entry == nil {
		return nil
	}
	copy := *entry
	copy.Event.Tags = append(gonostr.Tags(nil), entry.Event.Tags...)
	copy.Deliveries = append([]RelayDelivery(nil), entry.Deliveries...)
	return &copy
}

// RelayResult reports the latest durable state for one relay target.
type RelayResult struct {
	URL          string        `json:"url"`
	Required     bool          `json:"required"`
	Attempted    bool          `json:"attempted"`
	Acknowledged bool          `json:"acknowledged"`
	Attempts     int           `json:"attempts"`
	Duration     time.Duration `json:"duration,omitempty"`
	Error        string        `json:"error,omitempty"`
	CircuitOpen  bool          `json:"circuit_open,omitempty"`
	DeadLetter   bool          `json:"dead_letter,omitempty"`
}

type PublicationReport struct {
	EventID       string        `json:"event_id"`
	Quorum        int           `json:"quorum"`
	RequiredAcks  int           `json:"required_acks"`
	QuorumReached bool          `json:"quorum_reached"`
	Queued        bool          `json:"queued"`
	DeadLetter    bool          `json:"dead_letter"`
	Results       []RelayResult `json:"results"`
}

type QuorumError struct {
	Reports []PublicationReport
}

func (e *QuorumError) Error() string {
	if e == nil || len(e.Reports) == 0 {
		return "relay acknowledgement quorum not met"
	}
	r := e.Reports[0]
	return fmt.Sprintf("relay acknowledgement quorum not met for event %s: %d/%d required relays acknowledged", r.EventID, r.RequiredAcks, r.Quorum)
}

type RelayTransport interface {
	Publish(ctx context.Context, relayURL string, event gonostr.Event) error
}

type poolRelayTransport struct{ pool *gonostr.Pool }

func (t poolRelayTransport) Publish(ctx context.Context, relayURL string, event gonostr.Event) error {
	relay, err := t.pool.EnsureRelay(relayURL)
	if err != nil {
		return err
	}
	return relay.Publish(ctx, event)
}

type ReliablePublisherOptions struct {
	RequiredRelays      []string
	MirrorRelays        []string
	AckQuorum           int
	OutboxPath          string
	PublishTimeout      time.Duration
	BaseBackoff         time.Duration
	MaxBackoff          time.Duration
	MaxAttempts         int
	CircuitFailureLimit int
	CircuitCooldown     time.Duration
	DrainInterval       time.Duration

	transport RelayTransport
	now       func() time.Time
	jitter    func() float64
}

type circuitState struct {
	failures  int
	openUntil time.Time
}

type ReliablePublisher struct {
	opts       ReliablePublisherOptions
	store      *OutboxStore
	transport  RelayTransport
	pool       *gonostr.Pool
	deliveryMu sync.Mutex
	circuitMu  sync.Mutex
	circuits   map[string]circuitState

	retryTotal      atomic.Uint64
	deadLetterTotal atomic.Uint64
	publishedTotal  atomic.Uint64
	publishedMu     sync.RWMutex
	lastPublished   EventMarker
}

// EventMarker identifies the most recent event that reached authoritative
// publication quorum without exposing event content or signer material.
type EventMarker struct {
	ID string    `json:"id,omitempty"`
	At time.Time `json:"at,omitempty"`
}

// PublisherSnapshot is a redaction-safe view of durable publication health.
type PublisherSnapshot struct {
	QueueDepth      int                    `json:"queue_depth"`
	DeadLetters     int                    `json:"dead_letters"`
	RetryTotal      uint64                 `json:"retry_total"`
	DeadLetterTotal uint64                 `json:"dead_letter_total"`
	PublishedTotal  uint64                 `json:"published_total"`
	LastPublished   EventMarker            `json:"last_published,omitempty"`
	Circuits        []RelayCircuitSnapshot `json:"circuits"`
}

// RelayCircuitSnapshot omits relay errors and reports only breaker state.
type RelayCircuitSnapshot struct {
	URL       string    `json:"url"`
	Required  bool      `json:"required"`
	Open      bool      `json:"open"`
	OpenUntil time.Time `json:"open_until,omitempty"`
}

func NewReliablePublisher(opts ReliablePublisherOptions) (*ReliablePublisher, error) {
	opts.RequiredRelays = cleanRelayURLs(opts.RequiredRelays)
	opts.MirrorRelays = removeRelayURLs(cleanRelayURLs(opts.MirrorRelays), opts.RequiredRelays)
	if len(opts.RequiredRelays) == 0 {
		return nil, fmt.Errorf("at least one required relay is required")
	}
	if opts.AckQuorum <= 0 {
		opts.AckQuorum = len(opts.RequiredRelays)
	}
	if opts.AckQuorum > len(opts.RequiredRelays) {
		return nil, fmt.Errorf("relay acknowledgement quorum %d exceeds %d required relays", opts.AckQuorum, len(opts.RequiredRelays))
	}
	if opts.PublishTimeout <= 0 {
		opts.PublishTimeout = 10 * time.Second
	}
	if opts.BaseBackoff <= 0 {
		opts.BaseBackoff = time.Second
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = time.Minute
	}
	if opts.MaxBackoff < opts.BaseBackoff {
		return nil, fmt.Errorf("max backoff must be at least base backoff")
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 10
	}
	if opts.CircuitFailureLimit <= 0 {
		opts.CircuitFailureLimit = 3
	}
	if opts.CircuitCooldown <= 0 {
		opts.CircuitCooldown = 30 * time.Second
	}
	if opts.DrainInterval <= 0 {
		opts.DrainInterval = time.Second
	}
	if opts.now == nil {
		opts.now = func() time.Time { return time.Now().UTC() }
	}
	if opts.jitter == nil {
		opts.jitter = func() float64 { return 0.5 + rand.Float64() }
	}
	store, err := OpenOutbox(opts.OutboxPath)
	if err != nil {
		return nil, err
	}
	p := &ReliablePublisher{opts: opts, store: store, circuits: make(map[string]circuitState)}
	if opts.transport != nil {
		p.transport = opts.transport
	} else {
		p.pool = gonostr.NewPool()
		p.transport = poolRelayTransport{pool: p.pool}
	}
	return p, nil
}

func (p *ReliablePublisher) Close() {
	if p != nil && p.pool != nil {
		p.pool.Close("reliable publisher closed")
	}
}

// Snapshot reports durable queue, retry, dead-letter, publication, and circuit
// state for readiness and metrics. It is safe to call while the worker drains.
func (p *ReliablePublisher) Snapshot() (PublisherSnapshot, error) {
	if p == nil || p.store == nil {
		return PublisherSnapshot{}, fmt.Errorf("reliable publisher is not initialized")
	}
	entries, err := p.store.List()
	if err != nil {
		return PublisherSnapshot{}, err
	}
	snapshot := PublisherSnapshot{
		QueueDepth: len(entries), RetryTotal: p.retryTotal.Load(),
		DeadLetterTotal: p.deadLetterTotal.Load(), PublishedTotal: p.publishedTotal.Load(),
	}
	for _, entry := range entries {
		if entry.State == OutboxDeadLetter {
			snapshot.DeadLetters++
		}
	}
	p.publishedMu.RLock()
	snapshot.LastPublished = p.lastPublished
	p.publishedMu.RUnlock()
	now := p.opts.now()
	required := make(map[string]struct{}, len(p.opts.RequiredRelays))
	for _, url := range p.opts.RequiredRelays {
		required[url] = struct{}{}
	}
	p.circuitMu.Lock()
	for url, state := range p.circuits {
		_, isRequired := required[url]
		snapshot.Circuits = append(snapshot.Circuits, RelayCircuitSnapshot{
			URL: url, Required: isRequired, Open: state.openUntil.After(now), OpenUntil: state.openUntil,
		})
	}
	p.circuitMu.Unlock()
	sort.Slice(snapshot.Circuits, func(i, j int) bool { return snapshot.Circuits[i].URL < snapshot.Circuits[j].URL })
	return snapshot, nil
}

// Publish satisfies EventPublisher. Quorum failures are returned while signed
// events remain durably queued for background recovery.
func (p *ReliablePublisher) Publish(ctx context.Context, _ []string, signer Signer, events []*gonostr.Event) error {
	reports, err := p.PublishWithReport(ctx, signer, events)
	if err != nil {
		return err
	}
	for _, report := range reports {
		if !report.QuorumReached {
			return &QuorumError{Reports: reports}
		}
	}
	return nil
}

func (p *ReliablePublisher) PublishWithReport(ctx context.Context, signer Signer, events []*gonostr.Event) ([]PublicationReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	if signer == nil {
		return nil, fmt.Errorf("signer is nil")
	}
	for _, event := range events {
		if event == nil {
			continue
		}
		if err := signer.SignEvent(ctx, event); err != nil {
			return nil, fmt.Errorf("sign event kind %d: %w", event.Kind, err)
		}
	}
	return p.publishSigned(ctx, events)
}

func (p *ReliablePublisher) PublishSigned(ctx context.Context, events ...*gonostr.Event) ([]PublicationReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	return p.publishSigned(ctx, events)
}

func (p *ReliablePublisher) publishSigned(ctx context.Context, events []*gonostr.Event) ([]PublicationReport, error) {
	p.deliveryMu.Lock()
	defer p.deliveryMu.Unlock()
	reports := make([]PublicationReport, 0, len(events))
	for _, event := range events {
		if event == nil {
			continue
		}
		if event.ID == gonostr.ZeroID {
			event.ID = event.GetID()
		}
		now := p.opts.now()
		entry := &OutboxEntry{ID: event.ID.Hex(), Event: *event, Quorum: p.opts.AckQuorum, State: OutboxPending, CreatedAt: now, UpdatedAt: now}
		for _, url := range p.opts.RequiredRelays {
			entry.Deliveries = append(entry.Deliveries, RelayDelivery{URL: url, Required: true, NextAttempt: now})
		}
		for _, url := range p.opts.MirrorRelays {
			entry.Deliveries = append(entry.Deliveries, RelayDelivery{URL: url, NextAttempt: now})
		}
		stored, err := p.store.enqueue(entry)
		if err != nil {
			return reports, err
		}
		report, err := p.deliverEntry(ctx, stored)
		if err != nil {
			return reports, err
		}
		reports = append(reports, report)
	}
	for _, report := range reports {
		if !report.QuorumReached {
			return reports, &QuorumError{Reports: reports}
		}
	}
	return reports, nil
}

// DrainOnce attempts every due, non-dead-letter delivery in the durable spool.
func (p *ReliablePublisher) DrainOnce(ctx context.Context) ([]PublicationReport, error) {
	p.deliveryMu.Lock()
	defer p.deliveryMu.Unlock()
	entries, err := p.store.List()
	if err != nil {
		return nil, err
	}
	reports := make([]PublicationReport, 0, len(entries))
	for _, entry := range entries {
		report, err := p.deliverEntry(ctx, entry)
		if err != nil {
			return reports, err
		}
		reports = append(reports, report)
	}
	return reports, nil
}

// Run drains immediately on startup and periodically until ctx is canceled.
func (p *ReliablePublisher) Run(ctx context.Context) error {
	if _, err := p.DrainOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	ticker := time.NewTicker(p.opts.DrainInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := p.DrainOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
		}
	}
}

func (p *ReliablePublisher) deliverEntry(ctx context.Context, entry *OutboxEntry) (PublicationReport, error) {
	now := p.opts.now()
	wasQuorumReached := entry.QuorumReached
	results := make([]RelayResult, 0, len(entry.Deliveries))
	for i := range entry.Deliveries {
		delivery := &entry.Deliveries[i]
		result := RelayResult{URL: delivery.URL, Required: delivery.Required, Acknowledged: delivery.Acknowledged, Attempts: delivery.Attempts, Error: delivery.LastError, DeadLetter: delivery.DeadLetter}
		if delivery.Acknowledged || delivery.DeadLetter || delivery.NextAttempt.After(now) {
			results = append(results, result)
			continue
		}
		if until, open := p.circuitOpen(delivery.URL, now); open {
			result.CircuitOpen = true
			result.Error = fmt.Sprintf("relay circuit open until %s", until.UTC().Format(time.RFC3339Nano))
			results = append(results, result)
			continue
		}
		result.Attempted = true
		if delivery.Attempts > 0 {
			p.retryTotal.Add(1)
		}
		started := time.Now()
		err := p.publishWithTimeout(ctx, delivery.URL, entry.Event)
		result.Duration = time.Since(started)
		delivery.Attempts++
		delivery.LastAttempt = now
		result.Attempts = delivery.Attempts
		if err == nil {
			delivery.Acknowledged = true
			delivery.LastError = ""
			delivery.NextAttempt = time.Time{}
			p.recordCircuitSuccess(delivery.URL)
			result.Acknowledged = true
			result.Error = ""
		} else {
			delivery.LastError = err.Error()
			result.Error = delivery.LastError
			if delivery.Attempts >= p.opts.MaxAttempts {
				if !delivery.DeadLetter {
					p.deadLetterTotal.Add(1)
				}
				delivery.DeadLetter = true
				result.DeadLetter = true
			} else {
				delivery.NextAttempt = now.Add(p.backoff(delivery.Attempts))
			}
			p.recordCircuitFailure(delivery.URL, now)
		}
		entry.UpdatedAt = now
		if err := p.store.put(entry); err != nil {
			return PublicationReport{}, err
		}
		results = append(results, result)
	}
	entry.QuorumReached = requiredAcks(entry) >= entry.Quorum
	if entry.QuorumReached && !wasQuorumReached {
		p.publishedTotal.Add(1)
		p.publishedMu.Lock()
		p.lastPublished = EventMarker{ID: entry.ID, At: now}
		p.publishedMu.Unlock()
	}
	entry.State = stateFor(entry)
	entry.UpdatedAt = now
	allAcked := true
	for _, delivery := range entry.Deliveries {
		allAcked = allAcked && delivery.Acknowledged
	}
	report := reportFor(entry, results)
	if allAcked {
		if err := p.store.remove(entry.ID); err != nil {
			return PublicationReport{}, err
		}
		report.Queued = false
		return report, nil
	}
	if err := p.store.put(entry); err != nil {
		return PublicationReport{}, err
	}
	return report, nil
}

func stateFor(entry *OutboxEntry) OutboxState {
	for _, delivery := range entry.Deliveries {
		if !delivery.Acknowledged && delivery.DeadLetter {
			return OutboxDeadLetter
		}
	}
	return OutboxPending
}

func (p *ReliablePublisher) publishWithTimeout(ctx context.Context, relayURL string, event gonostr.Event) error {
	attemptCtx, cancel := context.WithTimeout(ctx, p.opts.PublishTimeout)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- p.transport.Publish(attemptCtx, relayURL, event) }()
	select {
	case err := <-result:
		return err
	case <-attemptCtx.Done():
		return attemptCtx.Err()
	}
}

func reportFor(entry *OutboxEntry, attempted []RelayResult) PublicationReport {
	byURL := make(map[string]RelayResult, len(attempted))
	for _, result := range attempted {
		byURL[result.URL] = result
	}
	results := make([]RelayResult, 0, len(entry.Deliveries))
	for _, delivery := range entry.Deliveries {
		result, ok := byURL[delivery.URL]
		if !ok {
			result = RelayResult{URL: delivery.URL, Required: delivery.Required}
		}
		result.Acknowledged = delivery.Acknowledged
		result.Attempts = delivery.Attempts
		result.DeadLetter = delivery.DeadLetter
		if result.Error == "" {
			result.Error = delivery.LastError
		}
		results = append(results, result)
	}
	return PublicationReport{EventID: entry.ID, Quorum: entry.Quorum, RequiredAcks: requiredAcks(entry), QuorumReached: entry.QuorumReached, Queued: true, DeadLetter: entry.State == OutboxDeadLetter, Results: results}
}

func requiredAcks(entry *OutboxEntry) int {
	count := 0
	for _, delivery := range entry.Deliveries {
		if delivery.Required && delivery.Acknowledged {
			count++
		}
	}
	return count
}

func (p *ReliablePublisher) backoff(attempt int) time.Duration {
	delay := p.opts.BaseBackoff
	for i := 1; i < attempt && delay < p.opts.MaxBackoff; i++ {
		if delay > p.opts.MaxBackoff/2 {
			delay = p.opts.MaxBackoff
			break
		}
		delay *= 2
	}
	if delay > p.opts.MaxBackoff {
		delay = p.opts.MaxBackoff
	}
	jitter := p.opts.jitter()
	if jitter < 0 {
		jitter = 0
	}
	return time.Duration(float64(delay) * jitter)
}

func (p *ReliablePublisher) circuitOpen(url string, now time.Time) (time.Time, bool) {
	p.circuitMu.Lock()
	defer p.circuitMu.Unlock()
	state := p.circuits[url]
	return state.openUntil, state.openUntil.After(now)
}

func (p *ReliablePublisher) recordCircuitSuccess(url string) {
	p.circuitMu.Lock()
	defer p.circuitMu.Unlock()
	delete(p.circuits, url)
}

func (p *ReliablePublisher) recordCircuitFailure(url string, now time.Time) {
	p.circuitMu.Lock()
	defer p.circuitMu.Unlock()
	state := p.circuits[url]
	state.failures++
	if state.failures >= p.opts.CircuitFailureLimit {
		state.failures = 0
		state.openUntil = now.Add(p.opts.CircuitCooldown)
	}
	p.circuits[url] = state
}

func cleanRelayURLs(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
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

func removeRelayURLs(in, remove []string) []string {
	removed := make(map[string]struct{}, len(remove))
	for _, value := range remove {
		removed[value] = struct{}{}
	}
	out := in[:0]
	for _, value := range in {
		if _, ok := removed[value]; !ok {
			out = append(out, value)
		}
	}
	return out
}
