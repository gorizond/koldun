package ingress

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gorizond/koldun/pkg/metrics"
)

// Ingress-side request coalescing.
//
// Identical chat requests that are retried while the first one is still being
// served attach to the in-flight response instead of enqueueing a second
// backlog message. Completed responses are kept for a short TTL so a retry
// after completion replays the cached result without touching the model.
//
// The coordinator is deliberately in-memory: koldun-ingress runs as a single
// replica, so no cross-replica locking or shared state is required. If the
// ingress is ever scaled out, this cache becomes per-replica and both
// coalescing and cache hits only apply to requests that land on the same pod
// (the cache key is still tenant-safe, so misses are the only consequence).

const (
	defaultDedupeTTL        = 10 * time.Minute
	defaultDedupeMaxEntries = 256
	defaultDedupeMaxBytes   = 32 << 20 // 32 MiB
	defaultDedupeMaxWaiters = 16
	dedupeWaiterBuffer      = 64
	dedupeStreamBufferBytes = 4 << 20 // per-entry stream buffer cap
	dedupeJSONBodyBytes     = 4 << 20 // non-stream bodies above this are not cached
	dedupeKeyDomain         = "koldun-req-v1\x00"
)

// volatileRequestKeys carry transport/session noise that must not split the
// request identity: two retries of the same prompt differ in these fields.
var volatileRequestKeys = []string{"user", "metadata", "request_id", "trace_id", "session_id"}

// requestDigest canonicalises a request body and hashes everything except the
// volatile top-level keys. Map keys are sorted by encoding/json, and
// json.Number keeps numeric literals stable across decode/encode.
func requestDigest(body []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()

	var envelope map[string]any
	if err := decoder.Decode(&envelope); err != nil {
		return "", fmt.Errorf("decode request body: %w", err)
	}
	for _, key := range volatileRequestKeys {
		delete(envelope, key)
	}
	canonical, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("canonicalise request body: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// requestDedupeKey binds the request digest to the conversation hash, which
// already contains the model, authenticated principal and stable chat headers.
// Two tenants can never coalesce because their conversation hashes differ.
func requestDedupeKey(conversationHash string, body []byte) (string, error) {
	digest, err := requestDigest(body)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(dedupeKeyDomain))
	_, _ = hash.Write([]byte(conversationHash))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(digest))
	return hex.EncodeToString(hash.Sum(nil)), nil
}

type dedupeEntryState uint8

const (
	dedupeStateInflight dedupeEntryState = iota
	dedupeStateCached
)

// dedupeBeginKind is the outcome of joining or creating a dedupe entry.
type dedupeBeginKind uint8

const (
	dedupeMiss dedupeBeginKind = iota
	dedupeAttach
	dedupeCacheHit
	dedupeRejected
)

func (k dedupeBeginKind) String() string {
	switch k {
	case dedupeAttach:
		return "attach"
	case dedupeCacheHit:
		return "cache_hit"
	case dedupeRejected:
		return "rejected"
	default:
		return "miss"
	}
}

// dedupeWaiter receives chunks only. Termination is signalled through the
// entry's done channel so it can never be lost when the chunk buffer is full.
type dedupeWaiter struct {
	ch      chan string
	dropped chan struct{}
	closed  bool
}

func newDedupeWaiter() *dedupeWaiter {
	return &dedupeWaiter{
		ch:      make(chan string, dedupeWaiterBuffer),
		dropped: make(chan struct{}),
	}
}

type dedupeEntry struct {
	key    string
	stream bool

	state       dedupeEntryState
	startedAt   time.Time
	expiresAt   time.Time
	terminal    bool
	terminalErr error

	// buffer holds normalised SSE payload strings (stream) or the single JSON
	// body (non-stream). It is the replay prefix for attach and cache hits.
	buffer      []string
	bufferBytes int64

	cacheable bool
	truncated bool

	waiters map[*dedupeWaiter]struct{}
	done    chan struct{}

	onTerminal func()

	lruElem *list.Element
}

// dedupeCoordinator is a mutex-guarded map of in-flight and cached entries.
// In-flight entries are never evicted; the LRU and byte budget only apply to
// completed (cached) entries.
type dedupeCoordinator struct {
	mu sync.Mutex

	entries map[string]*dedupeEntry
	lru     *list.List // front = most recently used cached entry

	cachedCount   int
	cachedBytes   int64
	inflightBytes int64

	ttl         time.Duration
	maxEntries  int
	maxBytes    int64
	maxWaiters  int
	gaugeSynced int
}

func newDedupeCoordinator(ttl time.Duration, maxEntries int, maxBytes int64, maxWaiters int) *dedupeCoordinator {
	if ttl <= 0 {
		ttl = defaultDedupeTTL
	}
	if maxEntries <= 0 {
		maxEntries = defaultDedupeMaxEntries
	}
	if maxBytes <= 0 {
		maxBytes = defaultDedupeMaxBytes
	}
	if maxWaiters <= 0 {
		maxWaiters = defaultDedupeMaxWaiters
	}
	c := &dedupeCoordinator{
		entries:    make(map[string]*dedupeEntry),
		lru:        list.New(),
		ttl:        ttl,
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		maxWaiters: maxWaiters,
	}
	c.syncGaugeLocked()
	return c
}

// dedupeCoordinator lazily initialises the per-server coordinator. It must be
// safe for a zero-value Server (tests construct &Server{} directly).
func (s *Server) dedupeCoordinator() *dedupeCoordinator {
	s.dedupeState.once.Do(func() {
		s.dedupeState.coord = newDedupeCoordinator(
			s.cfg.DedupeTTL,
			s.cfg.DedupeMaxEntries,
			s.cfg.DedupeMaxBytes,
			s.cfg.DedupeMaxWaiters,
		)
	})
	return s.dedupeState.coord
}

// dedupeBegin joins an existing entry, reuses a cached result, or creates a new
// in-flight entry owned by the caller. The returned handle is always non-nil.
func (s *Server) dedupeBegin(key string, stream bool) *dedupeHandle {
	return s.dedupeCoordinator().begin(key, stream)
}

func (c *dedupeCoordinator) begin(key string, stream bool) *dedupeHandle {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[key]; ok {
		switch entry.state {
		case dedupeStateCached:
			if now.After(entry.expiresAt) {
				c.removeLocked(entry)
				break
			}
			c.lru.MoveToFront(entry.lruElem)
			snapshot := append([]string(nil), entry.buffer...)
			replay := entry.bufferBytes
			metrics.IngressDedupReplayBytesTotal.Add(float64(replay))
			return &dedupeHandle{
				coord:    c,
				entry:    entry,
				kind:     dedupeCacheHit,
				key:      key,
				snapshot: snapshot,
			}
		case dedupeStateInflight:
			if entry.truncated || len(entry.waiters) >= c.maxWaiters {
				return &dedupeHandle{coord: c, entry: entry, kind: dedupeRejected, key: key}
			}
			waiter := newDedupeWaiter()
			entry.waiters[waiter] = struct{}{}
			snapshot := append([]string(nil), entry.buffer...)
			replay := entry.bufferBytes
			metrics.IngressDedupReplayBytesTotal.Add(float64(replay))
			return &dedupeHandle{
				coord:    c,
				entry:    entry,
				waiter:   waiter,
				kind:     dedupeAttach,
				key:      key,
				snapshot: snapshot,
			}
		}
	}

	entry := &dedupeEntry{
		key:       key,
		stream:    stream,
		state:     dedupeStateInflight,
		startedAt: now,
		cacheable: true,
		waiters:   make(map[*dedupeWaiter]struct{}),
		done:      make(chan struct{}),
	}
	waiter := newDedupeWaiter()
	entry.waiters[waiter] = struct{}{}
	c.entries[key] = entry
	c.syncGaugeLocked()

	return &dedupeHandle{
		coord:  c,
		entry:  entry,
		waiter: waiter,
		kind:   dedupeMiss,
		key:    key,
	}
}

func (c *dedupeCoordinator) removeLocked(entry *dedupeEntry) {
	delete(c.entries, entry.key)
	if entry.lruElem != nil {
		c.lru.Remove(entry.lruElem)
		entry.lruElem = nil
	}
	switch entry.state {
	case dedupeStateCached:
		c.cachedCount--
		c.cachedBytes -= entry.bufferBytes
		if c.cachedCount < 0 {
			c.cachedCount = 0
		}
		if c.cachedBytes < 0 {
			c.cachedBytes = 0
		}
	case dedupeStateInflight:
		c.inflightBytes -= entry.bufferBytes
		if c.inflightBytes < 0 {
			c.inflightBytes = 0
		}
	}
	c.syncGaugeLocked()
}

func (c *dedupeCoordinator) syncGaugeLocked() {
	if c.gaugeSynced == len(c.entries) {
		return
	}
	c.gaugeSynced = len(c.entries)
	metrics.IngressDedupEntries.Set(float64(c.gaugeSynced))
}

// publish buffers a normalised payload and fans it out to live waiters.
// Delivery is non-blocking: a slow waiter is dropped instead of stalling the
// pump that owns the upstream subscription.
func (c *dedupeCoordinator) publish(entry *dedupeEntry, payload string) {
	if entry == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry.terminal {
		return
	}

	size := int64(len(payload))
	// Replay buffers are bounded per entry and against the same global byte
	// budget as the cache. Cached entries are evicted LRU-first to make room;
	// if in-flight bytes alone exceed the budget the entry stops buffering
	// (attaches are rejected), while live waiters keep streaming.
	if entry.stream {
		if !entry.truncated {
			if entry.bufferBytes+size > dedupeStreamBufferBytes || !c.reserveLocked(size) {
				entry.truncated = true
				entry.cacheable = false
			} else {
				entry.buffer = append(entry.buffer, payload)
				entry.bufferBytes += size
				c.inflightBytes += size
			}
		}
	} else if len(entry.buffer) == 0 {
		if !c.reserveLocked(size) {
			entry.truncated = true
			entry.cacheable = false
		} else {
			entry.buffer = append(entry.buffer, payload)
			entry.bufferBytes += size
			c.inflightBytes += size
			if entry.bufferBytes > dedupeJSONBodyBytes {
				entry.cacheable = false
			}
		}
	}

	for waiter := range entry.waiters {
		select {
		case waiter.ch <- payload:
		default:
			c.dropWaiterLocked(entry, waiter)
		}
	}
}

// reserveLocked makes room in the global byte budget for size in-flight bytes.
// Cached entries are evicted LRU-first; false means the in-flight set alone
// would exceed the budget and the caller must stop buffering.
func (c *dedupeCoordinator) reserveLocked(size int64) bool {
	for c.cachedBytes+c.inflightBytes+size > c.maxBytes && c.lru.Len() > 0 {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		c.removeLocked(oldest.Value.(*dedupeEntry))
	}
	return c.inflightBytes+size <= c.maxBytes
}

func (c *dedupeCoordinator) dropWaiterLocked(entry *dedupeEntry, waiter *dedupeWaiter) {
	if waiter.closed {
		return
	}
	waiter.closed = true
	delete(entry.waiters, waiter)
	close(waiter.dropped)
}

// complete marks the entry terminal and optionally publishes it to the short
// cache. cacheable=false is used for error payloads: a retry must enqueue a
// fresh request instead of replaying a sticky failure.
func (c *dedupeCoordinator) complete(entry *dedupeEntry, cacheable bool) {
	if entry == nil {
		return
	}
	c.mu.Lock()
	if entry.terminal {
		c.mu.Unlock()
		return
	}
	entry.terminal = true
	entry.terminalErr = nil
	close(entry.done)
	entry.waiters = nil

	onTerminal := entry.onTerminal

	if cacheable && entry.cacheable && !entry.truncated {
		c.inflightBytes -= entry.bufferBytes
		if c.inflightBytes < 0 {
			c.inflightBytes = 0
		}
		entry.state = dedupeStateCached
		entry.expiresAt = time.Now().Add(c.ttl)
		entry.lruElem = c.lru.PushFront(entry)
		c.cachedCount++
		c.cachedBytes += entry.bufferBytes
		c.evictLocked()
	} else {
		c.removeLocked(entry)
	}
	c.mu.Unlock()

	if onTerminal != nil {
		onTerminal()
	}
}

func (c *dedupeCoordinator) fail(entry *dedupeEntry, err error) {
	if entry == nil {
		return
	}
	c.mu.Lock()
	if entry.terminal {
		c.mu.Unlock()
		return
	}
	entry.terminal = true
	entry.terminalErr = err
	close(entry.done)
	entry.waiters = nil

	onTerminal := entry.onTerminal
	c.removeLocked(entry)
	c.mu.Unlock()

	if onTerminal != nil {
		onTerminal()
	}
}

// evictLocked drops least-recently-used cached entries until the count and
// byte budget fit. In-flight entries live in a separate state and are skipped.
func (c *dedupeCoordinator) evictLocked() {
	for (c.cachedCount > c.maxEntries || c.cachedBytes > c.maxBytes) && c.lru.Len() > 0 {
		oldest := c.lru.Back()
		if oldest == nil {
			return
		}
		entry := oldest.Value.(*dedupeEntry)
		c.removeLocked(entry)
	}
}

// dedupeHandle is the caller-side view of one dedupe entry. Owner handles may
// publish/complete; attach and hit handles only consume events or a snapshot.
type dedupeHandle struct {
	coord  *dedupeCoordinator
	entry  *dedupeEntry
	waiter *dedupeWaiter
	kind   dedupeBeginKind
	key    string

	snapshot []string
}

func (h *dedupeHandle) Kind() dedupeBeginKind {
	if h == nil {
		return dedupeRejected
	}
	return h.kind
}

// Snapshot returns the replay prefix captured atomically when the caller
// joined the entry (attach) or the full cached result (hit).
func (h *dedupeHandle) Snapshot() []string {
	if h == nil {
		return nil
	}
	return append([]string(nil), h.snapshot...)
}

// Events carries replay chunks for this waiter. Completion is signalled by
// Done, never by closing this channel, so a full buffer cannot hide the
// terminal frame.
func (h *dedupeHandle) Events() <-chan string {
	if h == nil || h.waiter == nil {
		return nil
	}
	return h.waiter.ch
}

// Dropped is closed when this waiter fell behind and was removed from the
// fan-out. The handler must emit an explicit error terminator.
func (h *dedupeHandle) Dropped() <-chan struct{} {
	if h == nil || h.waiter == nil {
		return nil
	}
	return h.waiter.dropped
}

// Done is closed exactly once when the entry completes or fails, independent
// of waiter buffer space.
func (h *dedupeHandle) Done() <-chan struct{} {
	if h == nil || h.entry == nil {
		return nil
	}
	return h.entry.done
}

// TerminalError reports why the entry failed. It is safe to call only after
// Done is closed.
func (h *dedupeHandle) TerminalError() error {
	if h == nil || h.coord == nil || h.entry == nil {
		return nil
	}
	h.coord.mu.Lock()
	defer h.coord.mu.Unlock()
	return h.entry.terminalErr
}

// Publish is called by the owner's pump with already-normalised payloads.
func (h *dedupeHandle) Publish(payload string) {
	if h == nil || h.entry == nil || h.kind != dedupeMiss {
		return
	}
	h.coord.publish(h.entry, payload)
}

func (h *dedupeHandle) Complete() {
	if h == nil || h.entry == nil || h.kind != dedupeMiss {
		return
	}
	h.coord.complete(h.entry, true)
}

// CompleteUncached terminates the entry without caching it. Used for error
// payloads so retries do not replay a sticky failure for the whole TTL.
func (h *dedupeHandle) CompleteUncached() {
	if h == nil || h.entry == nil || h.kind != dedupeMiss {
		return
	}
	h.coord.complete(h.entry, false)
}

func (h *dedupeHandle) Fail(err error) {
	if h == nil || h.entry == nil || h.kind != dedupeMiss {
		return
	}
	h.coord.fail(h.entry, err)
}

// Close detaches this waiter. The pump keeps running and can still populate
// the cache: a client disconnect must not abort inference.
func (h *dedupeHandle) Close() {
	if h == nil || h.coord == nil || h.waiter == nil {
		return
	}
	c := h.coord
	c.mu.Lock()
	defer c.mu.Unlock()
	if h.waiter.closed {
		return
	}
	h.waiter.closed = true
	if h.entry != nil && h.entry.waiters != nil {
		delete(h.entry.waiters, h.waiter)
	}
}

// SetOnTerminal registers the owner's completion callback (session load and
// conversation record updates). It runs exactly once, outside the coordinator
// lock, on both Complete and Fail.
func (h *dedupeHandle) SetOnTerminal(fn func()) {
	if h == nil || h.coord == nil || h.entry == nil {
		return
	}
	h.coord.mu.Lock()
	if h.entry.terminal {
		h.coord.mu.Unlock()
		if fn != nil {
			fn()
		}
		return
	}
	h.entry.onTerminal = fn
	h.coord.mu.Unlock()
}

func (h *dedupeHandle) KeyPrefix() string {
	if h == nil {
		return ""
	}
	if len(h.key) <= 8 {
		return h.key
	}
	return h.key[:8]
}

func (h *dedupeHandle) EntryAge() time.Duration {
	if h == nil || h.coord == nil || h.entry == nil {
		return 0
	}
	h.coord.mu.Lock()
	defer h.coord.mu.Unlock()
	if h.entry.startedAt.IsZero() {
		return 0
	}
	return time.Since(h.entry.startedAt)
}

func (h *dedupeHandle) BufferBytes() int64 {
	if h == nil || h.coord == nil || h.entry == nil {
		return 0
	}
	h.coord.mu.Lock()
	defer h.coord.mu.Unlock()
	return h.entry.bufferBytes
}

func (h *dedupeHandle) Waiters() int {
	if h == nil || h.coord == nil || h.entry == nil {
		return 0
	}
	h.coord.mu.Lock()
	defer h.coord.mu.Unlock()
	return len(h.entry.waiters)
}

// dedupeStream reports whether the entry is a streaming response.
func (h *dedupeHandle) dedupeStream() bool {
	if h == nil || h.entry == nil {
		return false
	}
	return h.entry.stream
}

// errDedupeOwnerAborted marks an owner handler that returned before handing the
// entry to the pump (queue configuration or publish failure).
var errDedupeOwnerAborted = errors.New("ingress: request owner aborted before publishing")
