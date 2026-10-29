package dispatcher

import (
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/metrics"
)

const (
	// defaultWorkerStaleAfter bounds how long a worker heartbeat is trusted.
	defaultWorkerStaleAfter = 90 * time.Second
	// maxConsecutiveFallbacks avoids re-pinning on every transient busy signal.
	maxConsecutiveFallbacks = 3
)

// selectWorkerFor picks a worker honouring the conversation pin.
// Outcomes: pinned, fallback, repin, no_pin, no_idle.
func (s *Server) selectWorkerFor(pin string) (string, string) {
	pin = strings.TrimSpace(pin)
	now := time.Now()
	staleAfter := s.staleAfter()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fallbacks == nil {
		s.fallbacks = make(map[string]int)
	}

	pinned, pinKnown := s.workers[pin]
	live := make([]string, 0, len(s.workers))
	for name, st := range s.workers {
		if workerLive(st, now, staleAfter) {
			live = append(live, name)
		}
	}
	sort.Strings(live)

	if pin != "" && pinKnown && workerLive(pinned, now, staleAfter) {
		delete(s.fallbacks, pin)
		return pin, "pinned"
	}

	if len(live) == 0 {
		return "", "no_idle"
	}
	// Keep the deterministic choice stable for a stable worker pool.
	candidate := live[int(fnv32a(s.cfg.Hash)%uint32(len(live)))]

	if pin == "" {
		return candidate, "no_pin"
	}

	if pinKnown && workerFresh(pinned, now, staleAfter) && strings.ToLower(pinned.state) != "error" {
		// The pinned worker is simply busy: use another one but keep the pin,
		// re-pinning only after repeated fallbacks to avoid thrashing.
		fallbacks := s.fallbacks[pin] + 1
		if fallbacks < maxConsecutiveFallbacks {
			s.fallbacks[pin] = fallbacks
			return candidate, "fallback"
		}
		delete(s.fallbacks, pin)
		return candidate, "repin"
	}

	delete(s.fallbacks, pin)
	return candidate, "repin"
}

func (s *Server) staleAfter() time.Duration {
	if s.cfg.WorkerStaleAfter > 0 {
		return s.cfg.WorkerStaleAfter
	}
	return defaultWorkerStaleAfter
}

func workerLive(st *workerState, now time.Time, staleAfter time.Duration) bool {
	if st == nil {
		return false
	}
	return strings.ToLower(st.state) == "idle" && st.active == 0 && workerFresh(st, now, staleAfter)
}

func workerFresh(st *workerState, now time.Time, staleAfter time.Duration) bool {
	if st == nil || st.lastHeartbeat.IsZero() {
		return false
	}
	return now.Sub(st.lastHeartbeat) <= staleAfter
}

func fnv32a(value string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	return h.Sum32()
}

// pinnedWorker returns the worker recorded on the conversation record.
// Any read/parse failure disables affinity for this request.
func (s *Server) pinnedWorker() string {
	if s.conversations == nil {
		return ""
	}
	entry, err := s.conversations.Get(conversation.DefaultTTLPrefix + s.cfg.Hash)
	if err != nil {
		return ""
	}
	record, err := conversation.ParseRecord(entry.Value())
	if err != nil {
		return ""
	}
	return strings.TrimSpace(record.PinnedDllama)
}

// pinWorker persists the worker affinity; failures are logged and non-fatal.
func (s *Server) pinWorker(worker string) {
	worker = strings.TrimSpace(worker)
	if s.conversations == nil || worker == "" {
		return
	}
	key := conversation.DefaultTTLPrefix + s.cfg.Hash
	_, err := conversation.UpdateRecord(s.conversations, key, func(record *conversation.Record) bool {
		if record.PinnedDllama == worker {
			return false
		}
		record.PinnedDllama = worker
		return true
	})
	if err != nil {
		s.log.WithError(err).WithField("worker", worker).Debug("update pinned worker")
	}
}

func recordAffinityMetrics(pin, outcome string) {
	metrics.DispatcherAffinityEventsTotal.WithLabelValues(outcome).Inc()
	switch outcome {
	case "fallback":
		metrics.DispatcherCacheEvictionsTotal.WithLabelValues("pinned_busy").Inc()
	case "repin":
		if pin != "" {
			metrics.DispatcherCacheEvictionsTotal.WithLabelValues("pinned_gone").Inc()
		}
	}
}
