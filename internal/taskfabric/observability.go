package taskfabric

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gonostr "fiatjaf.com/nostr"
	casnostr "git.sharegap.net/cascadia/cascadia-go/nostr"
	"github.com/chebizarro/nostrig/internal/durable"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

const (
	defaultOutboxCriticalThreshold = 1000
	readinessProbeInterval         = 5 * time.Second
	livenessStaleAfter             = 20 * time.Second
)

var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type readinessCheck struct {
	OK        bool      `json:"ok"`
	Reason    string    `json:"reason,omitempty"`
	CheckedAt time.Time `json:"checked_at,omitempty"`
}

type observedEvent struct {
	ID string    `json:"id,omitempty"`
	At time.Time `json:"at,omitempty"`
}

type latencyHistogram struct {
	Count   uint64
	Sum     float64
	Buckets []uint64
}

type observerCounters struct {
	Processed uint64 `json:"processed"`
	Published uint64 `json:"published"`
	Conflicts uint64 `json:"conflicts"`
	Denials   uint64 `json:"denials"`
	Replays   uint64 `json:"replays"`
}

type relayDiagnostic struct {
	URL         string    `json:"url"`
	Connected   bool      `json:"connected"`
	CircuitOpen bool      `json:"circuit_open,omitempty"`
	OpenUntil   time.Time `json:"open_until,omitempty"`
}

type publisherSnapshotter interface {
	Snapshot() (nip34.PublisherSnapshot, error)
}

type serviceObserver struct {
	mu sync.RWMutex
	wg sync.WaitGroup

	startedAt      time.Time
	heartbeat      time.Time
	checks         map[string]readinessCheck
	requiredRelays []string
	requiredQuorum int
	outboxCritical int
	relays         []relayDiagnostic
	outbox         nip34.PublisherSnapshot
	counters       observerCounters
	histograms     map[string]*latencyHistogram
	lastProcessed  observedEvent
	lastPublished  observedEvent
}

func newServiceObserver(requiredRelays []string, quorum, outboxCritical int) *serviceObserver {
	if quorum <= 0 {
		quorum = len(requiredRelays)
	}
	if outboxCritical <= 0 {
		outboxCritical = defaultOutboxCriticalThreshold
	}
	now := time.Now().UTC()
	o := &serviceObserver{
		startedAt: now, heartbeat: now, requiredRelays: append([]string(nil), requiredRelays...),
		requiredQuorum: quorum, outboxCritical: outboxCritical,
		checks: map[string]readinessCheck{}, histograms: map[string]*latencyHistogram{},
	}
	for _, name := range []string{"signer_connected", "relay_quorum_connected", "initial_backfill_complete", "durable_store_writable", "outbox_below_critical_threshold"} {
		o.checks[name] = readinessCheck{OK: false, Reason: "initializing", CheckedAt: now}
	}
	return o
}

func (o *serviceObserver) setCheck(name string, ok bool, reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if ok {
		reason = ""
	}
	o.checks[name] = readinessCheck{OK: ok, Reason: reason, CheckedAt: time.Now().UTC()}
}

func (o *serviceObserver) beat() {
	o.mu.Lock()
	o.heartbeat = time.Now().UTC()
	o.mu.Unlock()
}

func (o *serviceObserver) recordProcessed(event *gonostr.Event, elapsed time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.counters.Processed++
	if event != nil {
		o.lastProcessed = observedEvent{ID: event.ID.Hex(), At: time.Now().UTC()}
	}
	o.observeLatencyLocked("process", elapsed)
}

func (o *serviceObserver) recordPublished(events []*gonostr.Event, elapsed time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, event := range events {
		if event == nil {
			continue
		}
		o.counters.Published++
		o.lastPublished = observedEvent{ID: event.ID.Hex(), At: time.Now().UTC()}
	}
	o.observeLatencyLocked("publish", elapsed)
}

func (o *serviceObserver) observePublishLatency(elapsed time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.observeLatencyLocked("publish", elapsed)
}

func (o *serviceObserver) observeLatencyLocked(operation string, elapsed time.Duration) {
	h := o.histograms[operation]
	if h == nil {
		h = &latencyHistogram{Buckets: make([]uint64, len(latencyBuckets))}
		o.histograms[operation] = h
	}
	seconds := elapsed.Seconds()
	h.Count++
	h.Sum += seconds
	for i, upper := range latencyBuckets {
		if seconds <= upper {
			h.Buckets[i]++
		}
	}
}

func (o *serviceObserver) observeResponse(response *gonostr.Event) {
	if response == nil {
		return
	}
	var body struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(response.Content), &body) != nil || body.Error == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if body.Error.Code == conflictErrorCode {
		o.counters.Conflicts++
	}
}

func (o *serviceObserver) recordStage(stage string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch stage {
	case "request_id_conflict":
		o.counters.Conflicts++
	case "replay_expired":
		o.counters.Replays++
	}
}

func (o *serviceObserver) recordReplay() {
	o.mu.Lock()
	o.counters.Replays++
	o.mu.Unlock()
}

func (o *serviceObserver) updateOperationalState(relayConnected func(string) bool, publisher publisherSnapshotter) {
	var publication nip34.PublisherSnapshot
	var snapshotErr error
	if publisher == nil {
		snapshotErr = fmt.Errorf("durable outbox disabled")
	} else {
		publication, snapshotErr = publisher.Snapshot()
	}

	openCircuits := make(map[string]nip34.RelayCircuitSnapshot)
	for _, circuit := range publication.Circuits {
		if circuit.Required && circuit.Open {
			openCircuits[circuit.URL] = circuit
		}
	}
	connected := 0
	relays := make([]relayDiagnostic, 0, len(o.requiredRelays))
	for _, relay := range o.requiredRelays {
		isConnected := relayConnected != nil && relayConnected(relay)
		circuit, open := openCircuits[relay]
		if isConnected && !open {
			connected++
		}
		relays = append(relays, relayDiagnostic{
			URL: redactRelayURL(relay), Connected: isConnected, CircuitOpen: open, OpenUntil: circuit.OpenUntil,
		})
	}
	sort.Slice(relays, func(i, j int) bool { return relays[i].URL < relays[j].URL })

	o.mu.Lock()
	o.relays = relays
	if snapshotErr == nil {
		redactedPublication := publication
		redactedPublication.Circuits = append([]nip34.RelayCircuitSnapshot(nil), publication.Circuits...)
		for i := range redactedPublication.Circuits {
			redactedPublication.Circuits[i].URL = redactRelayURL(redactedPublication.Circuits[i].URL)
		}
		o.outbox = redactedPublication
		if publication.LastPublished.ID != "" {
			o.lastPublished = observedEvent{ID: publication.LastPublished.ID, At: publication.LastPublished.At}
		}
	}
	o.mu.Unlock()

	if connected >= o.requiredQuorum {
		o.setCheck("relay_quorum_connected", true, "")
	} else {
		o.setCheck("relay_quorum_connected", false, fmt.Sprintf("connected_relays_below_quorum:%d/%d", connected, o.requiredQuorum))
	}
	if snapshotErr != nil {
		o.setCheck("outbox_below_critical_threshold", false, "outbox_unavailable")
	} else if publication.QueueDepth >= o.outboxCritical {
		o.setCheck("outbox_below_critical_threshold", false, fmt.Sprintf("critical_queue_depth:%d", publication.QueueDepth))
	} else {
		o.setCheck("outbox_below_critical_threshold", true, "")
	}
}

func (o *serviceObserver) refresh(ctx context.Context, signer casnostr.Signer, relayConnected func(string) bool, durablePaths []string, publisher publisherSnapshotter) {
	o.beat()
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	_, err := signer.GetPublicKey(probeCtx)
	cancel()
	if err != nil {
		o.setCheck("signer_connected", false, "signer_probe_failed")
	} else {
		o.setCheck("signer_connected", true, "")
	}
	if len(durablePaths) == 0 {
		o.setCheck("durable_store_writable", false, "durable_store_disabled")
	} else if err := durable.CheckWritable(durablePaths...); err != nil {
		o.setCheck("durable_store_writable", false, "durable_write_probe_failed")
	} else {
		o.setCheck("durable_store_writable", true, "")
	}
	o.updateOperationalState(relayConnected, publisher)
}

func (o *serviceObserver) startHeartbeat(ctx context.Context) {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		ticker := time.NewTicker(readinessProbeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.beat()
			}
		}
	}()
}

func (o *serviceObserver) startMonitor(ctx context.Context, signer casnostr.Signer, relayConnected func(string) bool, durablePaths []string, publisher publisherSnapshotter) {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		o.refresh(ctx, signer, relayConnected, durablePaths, publisher)
		ticker := time.NewTicker(readinessProbeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.refresh(ctx, signer, relayConnected, durablePaths, publisher)
			}
		}
	}()
}

func (o *serviceObserver) wait() {
	o.wg.Wait()
}

type observedPublisher struct {
	next     EventPublisher
	observer *serviceObserver
}

func (p observedPublisher) Publish(ctx context.Context, relays []string, signer nip34.Signer, events []*gonostr.Event) error {
	started := time.Now()
	err := p.next.Publish(ctx, relays, signer, events)
	if err == nil {
		p.observer.recordPublished(events, time.Since(started))
	} else {
		p.observer.observePublishLatency(time.Since(started))
	}
	return err
}

type observedAuditSink struct {
	next     AuthzAuditSink
	observer *serviceObserver
}

func (s observedAuditSink) Record(ctx context.Context, record AuthzAuditRecord) error {
	if record.Decision == "deny" {
		s.observer.mu.Lock()
		s.observer.counters.Denials++
		s.observer.mu.Unlock()
	}
	return s.next.Record(ctx, record)
}

func (o *serviceObserver) errorReporter(logger *slog.Logger) serveErrorReporter {
	return func(stage string, err error, event *gonostr.Event) {
		if err == nil {
			return
		}
		o.recordStage(stage)
		attrs := []any{"stage", stage, "error", err.Error()}
		if event != nil {
			attrs = append(attrs, "event_id", event.ID.Hex(), "event_kind", int(event.Kind))
		}
		logger.Error("serve operation failed", attrs...)
	}
}

func newServeLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func (o *serviceObserver) readyLocked() bool {
	for _, name := range []string{"signer_connected", "relay_quorum_connected", "initial_backfill_complete", "durable_store_writable", "outbox_below_critical_threshold"} {
		if !o.checks[name].OK {
			return false
		}
	}
	return true
}

func (o *serviceObserver) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", o.handleLive)
	mux.HandleFunc("GET /readyz", o.handleReady)
	mux.HandleFunc("GET /metrics", o.handleMetrics)
	mux.HandleFunc("GET /diagnostics", o.handleDiagnostics)
	return mux
}

func (o *serviceObserver) handleLive(w http.ResponseWriter, _ *http.Request) {
	o.mu.RLock()
	heartbeat := o.heartbeat
	o.mu.RUnlock()
	live := time.Since(heartbeat) <= livenessStaleAfter
	status := http.StatusOK
	state := "live"
	if !live {
		status, state = http.StatusServiceUnavailable, "stale"
	}
	writeJSON(w, status, map[string]any{"status": state, "heartbeat": heartbeat})
}

func (o *serviceObserver) handleReady(w http.ResponseWriter, _ *http.Request) {
	o.mu.RLock()
	ready := o.readyLocked()
	checks := cloneChecks(o.checks)
	o.mu.RUnlock()
	status := http.StatusOK
	state := "ready"
	if !ready {
		status, state = http.StatusServiceUnavailable, "not_ready"
	}
	writeJSON(w, status, map[string]any{"status": state, "checks": checks})
}

func (o *serviceObserver) handleDiagnostics(w http.ResponseWriter, _ *http.Request) {
	o.mu.RLock()
	out := map[string]any{
		"ready": o.readyLocked(), "uptime_seconds": time.Since(o.startedAt).Seconds(),
		"checks": cloneChecks(o.checks), "required_relay_quorum": o.requiredQuorum,
		"relays": append([]relayDiagnostic(nil), o.relays...), "outbox": o.outbox,
		"counters": o.counters, "last_processed": o.lastProcessed, "last_published": o.lastPublished,
	}
	o.mu.RUnlock()
	writeJSON(w, http.StatusOK, out)
}

func (o *serviceObserver) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	o.mu.RLock()
	counters := o.counters
	outbox := o.outbox
	lastProcessed, lastPublished := o.lastProcessed, o.lastPublished
	histograms := make(map[string]latencyHistogram, len(o.histograms))
	for name, h := range o.histograms {
		histograms[name] = latencyHistogram{Count: h.Count, Sum: h.Sum, Buckets: append([]uint64(nil), h.Buckets...)}
	}
	ready := o.readyLocked()
	o.mu.RUnlock()

	var b bytes.Buffer
	writeMetric(&b, "nostrig_events_processed_total", counters.Processed)
	published := counters.Published
	if outbox.PublishedTotal > published {
		published = outbox.PublishedTotal
	}
	writeMetric(&b, "nostrig_events_published_total", published)
	writeMetric(&b, "nostrig_conflicts_total", counters.Conflicts)
	writeMetric(&b, "nostrig_denials_total", counters.Denials)
	writeMetric(&b, "nostrig_retries_total", outbox.RetryTotal)
	writeMetric(&b, "nostrig_dead_letters_total", outbox.DeadLetterTotal)
	writeMetric(&b, "nostrig_replays_total", counters.Replays)
	writeMetric(&b, "nostrig_outbox_queue_depth", uint64(outbox.QueueDepth))
	writeMetric(&b, "nostrig_outbox_dead_letter_depth", uint64(outbox.DeadLetters))
	if ready {
		writeMetric(&b, "nostrig_ready", 1)
	} else {
		writeMetric(&b, "nostrig_ready", 0)
	}
	writeEventMetrics(&b, "processed", lastProcessed)
	writeEventMetrics(&b, "published", lastPublished)
	names := make([]string, 0, len(histograms))
	for name := range histograms {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		h := histograms[name]
		for i, upper := range latencyBuckets {
			fmt.Fprintf(&b, "nostrig_operation_duration_seconds_bucket{operation=%s,le=%s} %d\n", strconv.Quote(name), strconv.Quote(strconv.FormatFloat(upper, 'g', -1, 64)), h.Buckets[i])
		}
		fmt.Fprintf(&b, "nostrig_operation_duration_seconds_bucket{operation=%s,le=\"+Inf\"} %d\n", strconv.Quote(name), h.Count)
		fmt.Fprintf(&b, "nostrig_operation_duration_seconds_sum{operation=%s} %g\n", strconv.Quote(name), h.Sum)
		fmt.Fprintf(&b, "nostrig_operation_duration_seconds_count{operation=%s} %d\n", strconv.Quote(name), h.Count)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b.Bytes())
}

func startObservabilityHTTP(ctx context.Context, addr string, observer *serviceObserver, logger *slog.Logger) (func(), error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return func() {}, nil
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on observability address: %w", err)
	}
	server := &http.Server{Handler: observer.handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("observability server failed", "error", err.Error())
		}
	}()
	logger.Info("observability server started", "address", listener.Addr().String())
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}, nil
}

func cloneChecks(in map[string]readinessCheck) map[string]readinessCheck {
	out := make(map[string]readinessCheck, len(in))
	for name, check := range in {
		out[name] = check
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeMetric(b *bytes.Buffer, name string, value uint64) {
	fmt.Fprintf(b, "%s %d\n", name, value)
}

func writeEventMetrics(b *bytes.Buffer, kind string, event observedEvent) {
	if event.At.IsZero() {
		return
	}
	fmt.Fprintf(b, "nostrig_last_%s_event_timestamp_seconds %d\n", kind, event.At.Unix())
	fmt.Fprintf(b, "nostrig_last_%s_event_info{event_id=%s} 1\n", kind, strconv.Quote(event.ID))
}

func redactRelayURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "<redacted>"
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	if parsed.Path != "" && parsed.Path != "/" {
		parsed.Path = "/<redacted>"
	}
	return parsed.String()
}
