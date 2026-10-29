package llm

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/metrics"
)

const (
	chatCacheMaxEntries = 256
	chatCacheTTL        = 10 * time.Minute

	dedupMaxEntries = 512
	dedupTTL        = 10 * time.Minute
	dedupMaxBytes   = 256 << 10

	defaultMaxSeqLen = 4096
)

// errStreamCommitted marks failures where the response was already partly
// delivered, so retrying would duplicate text.
var errStreamCommitted = errors.New("stream committed")

var contextOverflowPattern = regexp.MustCompile(`(?i)(context.length|maximum context|too many tokens|max_seq_len|exceed.*(token|context))`)

func isContextOverflowText(text string) bool {
	return contextOverflowPattern.MatchString(text)
}

// chatEntry remembers the last request served for a conversation.
type chatEntry struct {
	messages int
	hash     string
	at       time.Time
}

type chatRecord struct {
	key   string
	entry chatEntry
}

// chatCache is a bounded LRU mapping conversation identity to the last served
// message history.
type chatCache struct {
	mu      sync.Mutex
	max     int
	ttl     time.Duration
	now     func() time.Time
	lru     *list.List
	items   map[string]*list.Element
	hits    int64
	lookups int64
}

func newChatCache(max int, ttl time.Duration) *chatCache {
	if max <= 0 {
		max = chatCacheMaxEntries
	}
	if ttl <= 0 {
		ttl = chatCacheTTL
	}
	return &chatCache{
		max:   max,
		ttl:   ttl,
		now:   time.Now,
		lru:   list.New(),
		items: make(map[string]*list.Element),
	}
}

// Lookup returns the cached history and records hit-ratio statistics.
func (c *chatCache) Lookup(key string) (chatEntry, bool) {
	if c == nil || key == "" {
		return chatEntry{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lookups++
	element, ok := c.items[key]
	if !ok {
		return chatEntry{}, false
	}
	record := element.Value.(*chatRecord)
	if c.now().Sub(record.entry.at) > c.ttl {
		c.lru.Remove(element)
		delete(c.items, key)
		return chatEntry{}, false
	}
	c.lru.MoveToFront(element)
	c.hits++
	return record.entry, true
}

// Store records the history served for a conversation.
func (c *chatCache) Store(key string, messages int, hash string) {
	if c == nil || key == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, ok := c.items[key]; ok {
		record := element.Value.(*chatRecord)
		record.entry = chatEntry{messages: messages, hash: hash, at: c.now()}
		c.lru.MoveToFront(element)
		return
	}
	record := &chatRecord{key: key, entry: chatEntry{messages: messages, hash: hash, at: c.now()}}
	c.items[key] = c.lru.PushFront(record)
	for len(c.items) > c.max {
		c.evictOldestLocked()
	}
}

// Ratio reports hits/lookups, or 0 when no lookup happened yet.
func (c *chatCache) Ratio() float64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lookups == 0 {
		return 0
	}
	return float64(c.hits) / float64(c.lookups)
}

func (c *chatCache) evictOldestLocked() {
	element := c.lru.Back()
	if element == nil {
		return
	}
	record := element.Value.(*chatRecord)
	c.lru.Remove(element)
	delete(c.items, record.key)
}

type dedupState string

const (
	dedupInProgress dedupState = "in_progress"
	dedupDone       dedupState = "done"
)

type dedupEntry struct {
	key       string
	subject   string
	state     dedupState
	chunks    [][]byte
	bytes     int
	truncated bool
	at        time.Time
}

// dedupCache suppresses duplicate assignments for the same request id.
type dedupCache struct {
	mu       sync.Mutex
	max      int
	ttl      time.Duration
	maxBytes int
	now      func() time.Time
	lru      *list.List
	items    map[string]*list.Element
}

func newDedupCache(max int, ttl time.Duration, maxBytes int) *dedupCache {
	if max <= 0 {
		max = dedupMaxEntries
	}
	if ttl <= 0 {
		ttl = dedupTTL
	}
	if maxBytes <= 0 {
		maxBytes = dedupMaxBytes
	}
	return &dedupCache{
		max:      max,
		ttl:      ttl,
		maxBytes: maxBytes,
		now:      time.Now,
		lru:      list.New(),
		items:    make(map[string]*list.Element),
	}
}

// Begin reserves the request id. When the request already completed, replay
// holds the response chunks and proceed is false. When it is still running or
// the replay is unusable, replay is nil and proceed is false.
func (d *dedupCache) Begin(key, subject string) (replay [][]byte, proceed bool) {
	if d == nil || key == "" {
		return nil, true
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	if element, ok := d.items[key]; ok {
		entry := element.Value.(*dedupEntry)
		if d.now().Sub(entry.at) > d.ttl {
			d.lru.Remove(element)
			delete(d.items, key)
		} else if entry.state == dedupDone && entry.subject == subject && !entry.truncated {
			d.lru.MoveToFront(element)
			return cloneChunks(entry.chunks), false
		} else {
			d.lru.MoveToFront(element)
			return nil, false
		}
	}

	entry := &dedupEntry{key: key, subject: subject, state: dedupInProgress, at: d.now()}
	d.items[key] = d.lru.PushFront(entry)
	d.evictLocked()
	return nil, true
}

// Complete marks the request as done and stores its response for replay.
func (d *dedupCache) Complete(key string, chunks [][]byte, truncated bool) {
	if d == nil || key == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	element, ok := d.items[key]
	if !ok {
		return
	}
	entry := element.Value.(*dedupEntry)
	entry.state = dedupDone
	entry.at = d.now()

	total := 0
	for _, chunk := range chunks {
		total += len(chunk)
	}
	if truncated || total > d.maxBytes {
		entry.truncated = true
		entry.chunks = nil
		entry.bytes = 0
		return
	}
	entry.chunks = cloneChunks(chunks)
	entry.bytes = total
}

// Fail drops the entry so the request stays retryable.
func (d *dedupCache) Fail(key string) {
	if d == nil || key == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if element, ok := d.items[key]; ok {
		d.lru.Remove(element)
		delete(d.items, key)
	}
}

func (d *dedupCache) evictLocked() {
	for _, key := range d.expiredKeysLocked() {
		if element, ok := d.items[key]; ok {
			d.lru.Remove(element)
			delete(d.items, key)
		}
	}
	for len(d.items) > d.max {
		if !d.evictOldestDoneLocked() {
			return
		}
	}
}

func (d *dedupCache) expiredKeysLocked() []string {
	now := d.now()
	var keys []string
	for key, element := range d.items {
		entry := element.Value.(*dedupEntry)
		if now.Sub(entry.at) > d.ttl {
			keys = append(keys, key)
		}
	}
	return keys
}

func (d *dedupCache) evictOldestDoneLocked() bool {
	for element := d.lru.Back(); element != nil; element = element.Prev() {
		entry := element.Value.(*dedupEntry)
		if entry.state != dedupDone {
			continue
		}
		d.lru.Remove(element)
		delete(d.items, entry.key)
		return true
	}
	return false
}

func cloneChunks(chunks [][]byte) [][]byte {
	if len(chunks) == 0 {
		return nil
	}
	out := make([][]byte, len(chunks))
	for i, chunk := range chunks {
		out[i] = append([]byte(nil), chunk...)
	}
	return out
}

// chunkRecorder captures published response chunks for dedup replay and tracks
// whether the first token already reached the client.
type chunkRecorder struct {
	mu         sync.Mutex
	chunks     [][]byte
	bytes      int
	truncated  bool
	firstToken bool
	coldStart  bool
	startedAt  time.Time
}

func (r *chunkRecorder) record(chunk []byte) {
	if r == nil || len(chunk) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.truncated {
		if r.bytes+len(chunk) > dedupMaxBytes {
			r.truncated = true
			r.chunks = nil
			r.bytes = 0
		} else {
			r.chunks = append(r.chunks, append([]byte(nil), chunk...))
			r.bytes += len(chunk)
		}
	}
	if !r.firstToken {
		r.firstToken = true
		if r.coldStart && !r.startedAt.IsZero() {
			metrics.ColdStartSeconds.Observe(time.Since(r.startedAt).Seconds())
		}
	}
}

func (r *chunkRecorder) hasToken() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.firstToken
}

func (r *chunkRecorder) snapshot() ([][]byte, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneChunks(r.chunks), r.truncated
}

// historyClass captures the prefix-cache decision for one request.
type historyClass struct {
	result    string // hit | miss | cold_start | truncated
	kind      string // cold_start | incremental | full
	reason    string
	cold      bool
	truncated string
	sent      int
}

func chatCacheKey(payload *inboundRequest) string {
	if payload == nil {
		return ""
	}
	chatID := strings.TrimSpace(payload.ChatID)
	hash := strings.TrimSpace(payload.Hash)
	if chatID == "" || hash == "" {
		return ""
	}
	return chatID + "|" + hash
}

func messageCount(payload *inboundRequest) int {
	if payload == nil {
		return 0
	}
	if len(payload.Raw) > 0 {
		if count, err := conversation.MessageCount(payload.Raw); err == nil {
			return count
		}
	}
	return len(payload.Request.Messages)
}

func prefixHash(payload *inboundRequest, n int) string {
	if payload == nil || len(payload.Raw) == 0 || n <= 0 {
		return ""
	}
	hash, err := conversation.PrefixHash(payload.Raw, n)
	if err != nil {
		return ""
	}
	return hash
}

// classify decides whether the request can reuse the warm prefix.
func (s *Server) classify(payload *inboundRequest) historyClass {
	count := messageCount(payload)
	cursor := payload.Cursor
	if cursor < 0 {
		cursor = 0
	}

	class := historyClass{result: "miss", kind: "full", sent: count}

	// The client-declared prefix must be hashable and match the record hash.
	prefixValid := cursor > 0 && payload.PrefixHash != "" && prefixHash(payload, cursor) == payload.PrefixHash
	if entry, ok := s.chats.Lookup(chatCacheKey(payload)); ok && prefixValid && entry.messages == cursor && entry.hash == payload.PrefixHash {
		class.result = "hit"
		class.kind = "incremental"
		class.sent = count - cursor
		if class.sent < 0 {
			class.sent = 0
		}
		return class
	}

	switch {
	case cursor > 0 && count == 1:
		class.truncated = "new_message_only"
	case count < cursor:
		class.truncated = "history_shrunk"
	}
	if class.truncated != "" {
		class.result = "truncated"
		class.sent = count
		return class
	}

	if cursor > 0 || count > 1 {
		class.result = "cold_start"
		class.kind = "cold_start"
		class.cold = true
		class.sent = count
		if cursor > 0 {
			class.reason = "instance_change"
		} else {
			class.reason = "record_expired"
		}
	}
	return class
}

// rememberChat stores the history served for a successful request.
func (s *Server) rememberChat(payload *inboundRequest) {
	key := chatCacheKey(payload)
	if key == "" {
		return
	}
	count := messageCount(payload)
	s.chats.Store(key, count, prefixHash(payload, count))
}

func (s *Server) recordHistoryMetrics(payload *inboundRequest, class historyClass) {
	metrics.SessionMessagesSentTotal.WithLabelValues(class.result).Inc()
	metrics.SessionHistoryMessages.WithLabelValues(class.kind).Observe(float64(class.sent))
	metrics.CacheEventsTotal.WithLabelValues(class.result).Inc()
	metrics.CacheHitRatio.Set(s.chats.Ratio())
	if class.truncated != "" {
		metrics.ContextTruncationsTotal.WithLabelValues(class.truncated).Inc()
	}
	if class.cold {
		metrics.ColdStartsTotal.WithLabelValues(class.reason).Inc()
	}

	s.log.WithFields(map[string]any{
		"chatId":          payload.ChatID,
		"result":          class.result,
		"historyMessages": class.sent,
		"cursor":          payload.Cursor,
		"coldStart":       class.cold,
	}).Info("session history classified")
	if class.cold {
		s.log.WithFields(map[string]any{
			"reason":           class.reason,
			"replayedMessages": class.sent,
		}).Info("cold start: replaying client history")
	}
}

// publishChunk publishes a response chunk and records it for dedup replay.
func (s *Server) publishChunk(target string, recorder *chunkRecorder, chunk []byte) error {
	if err := s.nc.Publish(target, chunk); err != nil {
		return err
	}
	recorder.record(chunk)
	return nil
}

func (s *Server) publishStreamInterrupted(target string, cause error, recorder *chunkRecorder) {
	payload := map[string]any{
		"error": map[string]any{
			"message": fmt.Sprintf("upstream stream interrupted: %v", cause),
			"type":    "server_error",
			"code":    "stream_interrupted",
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	if err := s.publishChunk(target, recorder, body); err != nil {
		s.log.WithError(err).Warn("publish stream error chunk")
	}
	if err := s.nc.Publish(target, []byte("[DONE]")); err != nil {
		s.log.WithError(err).Warn("publish stream interrupted done marker")
	}
}

func contextOverflowPayload(maxSeqLen int) []byte {
	if maxSeqLen <= 0 {
		maxSeqLen = defaultMaxSeqLen
	}
	payload := map[string]any{
		"error": map[string]any{
			"message": fmt.Sprintf("context_length_exceeded: maximum context length is %d tokens", maxSeqLen),
			"type":    "invalid_request_error",
			"param":   nil,
			"code":    "context_length_exceeded",
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return body
}

func (s *Server) publishContextOverflow(target string, recorder *chunkRecorder) {
	body := contextOverflowPayload(s.cfg.MaxSeqLen)
	metrics.ContextOverflowTotal.WithLabelValues("sidecar").Inc()
	if body != nil {
		if err := s.publishChunk(target, recorder, body); err != nil {
			s.log.WithError(err).Warn("publish context overflow chunk")
		}
	}
	if err := s.nc.Publish(target, []byte("[DONE]")); err != nil {
		s.log.WithError(err).Warn("publish context overflow done marker")
	}
}

// streamingErrorText extracts the error message from a JSON chunk, if any.
func streamingErrorText(data []byte) string {
	var envelope struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return ""
	}
	if len(envelope.Error) == 0 || string(envelope.Error) == "null" {
		return ""
	}
	var object struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(envelope.Error, &object); err == nil && strings.TrimSpace(object.Message) != "" {
		return object.Message
	}
	var text string
	if err := json.Unmarshal(envelope.Error, &text); err == nil {
		return text
	}
	return string(envelope.Error)
}
