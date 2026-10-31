package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// NATSOperationsTotal tracks total NATS operations by type and status
	NATSOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "nats",
			Name:      "operations_total",
			Help:      "Total number of NATS operations by type and status",
		},
		[]string{"operation", "status"},
	)

	// NATSOperationDuration tracks duration of NATS operations
	NATSOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "koldun",
			Subsystem: "nats",
			Name:      "operation_duration_seconds",
			Help:      "Duration of NATS operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	// NATSRetries tracks retry attempts for NATS operations
	NATSRetries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "nats",
			Name:      "retries_total",
			Help:      "Total number of NATS operation retries",
		},
		[]string{"operation"},
	)

	// NATSConnectionStatus tracks current NATS connection status
	NATSConnectionStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "koldun",
			Subsystem: "nats",
			Name:      "connection_status",
			Help:      "NATS connection status (1=connected, 0=disconnected)",
		},
		[]string{"server"},
	)

	// NATSMessagesPublished tracks messages published to NATS
	NATSMessagesPublished = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "nats",
			Name:      "messages_published_total",
			Help:      "Total number of messages published to NATS",
		},
		[]string{"subject"},
	)

	// NATSMessagesReceived tracks messages received from NATS
	NATSMessagesReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "nats",
			Name:      "messages_received_total",
			Help:      "Total number of messages received from NATS",
		},
		[]string{"subject"},
	)

	// NATSKVOperations tracks KV store operations
	NATSKVOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "nats",
			Name:      "kv_operations_total",
			Help:      "Total number of NATS KV operations",
		},
		[]string{"operation", "bucket", "status"},
	)

	// NATSKVOperationDuration tracks KV operation duration
	NATSKVOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "koldun",
			Subsystem: "nats",
			Name:      "kv_operation_duration_seconds",
			Help:      "Duration of NATS KV operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"operation", "bucket"},
	)

	// DispatcherWorkersActive tracks active workers in dispatcher
	DispatcherWorkersActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "koldun",
			Subsystem: "dispatcher",
			Name:      "workers_active",
			Help:      "Number of active workers in dispatcher",
		},
	)

	// DispatcherAssignmentsInflight tracks inflight assignments
	DispatcherAssignmentsInflight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "koldun",
			Subsystem: "dispatcher",
			Name:      "assignments_inflight",
			Help:      "Number of inflight assignments in dispatcher",
		},
	)

	// DispatcherBacklogSize tracks backlog queue size
	DispatcherBacklogSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "koldun",
			Subsystem: "dispatcher",
			Name:      "backlog_size",
			Help:      "Size of dispatcher backlog queue",
		},
	)

	// IngressRequestsTotal tracks total ingress requests
	IngressRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "ingress",
			Name:      "requests_total",
			Help:      "Total number of ingress requests",
		},
		[]string{"method", "path", "status"},
	)

	// IngressRequestDuration tracks ingress request duration
	IngressRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "koldun",
			Subsystem: "ingress",
			Name:      "request_duration_seconds",
			Help:      "Duration of ingress requests in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	// IngressDedupTotal tracks ingress request coalescing outcomes
	// (miss, attach, cache_hit, rejected).
	IngressDedupTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "ingress",
			Name:      "dedup_total",
			Help:      "Ingress request coalescing outcomes",
		},
		[]string{"result"},
	)

	// IngressDedupWaiters tracks the number of waiters attached to a deduped
	// request when the request joins or creates an entry.
	IngressDedupWaiters = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "koldun",
			Subsystem: "ingress",
			Name:      "dedup_waiters",
			Help:      "Waiters attached to a deduped ingress entry",
			Buckets:   []float64{0, 1, 2, 3, 4, 8, 16, 32},
		},
	)

	// IngressDedupReplayBytesTotal tracks bytes replayed from an in-flight
	// buffer or a cached result.
	IngressDedupReplayBytesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "ingress",
			Name:      "dedup_replay_bytes_total",
			Help:      "Bytes replayed from ingress dedupe buffers",
		},
	)

	// IngressDedupEntries tracks entries currently held by the dedupe
	// coordinator (in-flight and cached).
	IngressDedupEntries = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "koldun",
			Subsystem: "ingress",
			Name:      "dedup_entries",
			Help:      "Entries held by the ingress dedupe coordinator",
		},
	)

	// LLMRequestsActive tracks active LLM requests
	LLMRequestsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "koldun",
			Subsystem: "llm",
			Name:      "requests_active",
			Help:      "Number of active LLM requests",
		},
	)

	// LLMRequestsTotal tracks total LLM requests
	LLMRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "llm",
			Name:      "requests_total",
			Help:      "Total number of LLM requests",
		},
		[]string{"status"},
	)

	// LLMRequestDuration tracks LLM request duration
	LLMRequestDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "koldun",
			Subsystem: "llm",
			Name:      "request_duration_seconds",
			Help:      "Duration of LLM requests in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.1, 2, 10), // 0.1s to ~102s
		},
	)

	// LLMSidecarHealthStatus tracks sidecar health status
	LLMSidecarHealthStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "koldun",
			Subsystem: "llm",
			Name:      "sidecar_health_status",
			Help:      "LLM sidecar health status (1=healthy, 0=unhealthy)",
		},
		[]string{"dllama"},
	)

	// SessionMessagesSentTotal tracks per-request history decisions (hit, miss,
	// cold_start, truncated).
	SessionMessagesSentTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Name:      "session_messages_sent_total",
			Help:      "Chat requests by prefix-cache decision",
		},
		[]string{"result"},
	)

	// SessionHistoryMessages tracks the number of client history messages
	// forwarded to the sidecar, labelled by request classification.
	SessionHistoryMessages = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "koldun",
			Name:      "session_history_messages",
			Help:      "Number of client history messages forwarded to the model",
			Buckets:   prometheus.ExponentialBuckets(2, 2, 10),
		},
		[]string{"kind"},
	)

	// CacheEventsTotal tracks prefix-cache bookkeeping events.
	CacheEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Name:      "cache_events_total",
			Help:      "Prefix-cache events by result",
		},
		[]string{"result"},
	)

	// CacheHitRatio tracks the observed prefix-cache hit ratio (0..1).
	CacheHitRatio = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "koldun",
			Name:      "cache_hit_ratio",
			Help:      "Share of requests served from the warm prefix cache",
		},
	)

	// ColdStartSeconds tracks the time from request arrival to the first token
	// when the worker has to replay client history.
	ColdStartSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "koldun",
			Name:      "cold_start_seconds",
			Help:      "Duration from request arrival to first token on cold start",
			Buckets:   []float64{1, 2, 5, 10, 20, 30, 60, 90, 120, 180, 240, 300},
		},
	)

	// ColdStartsTotal tracks cold starts by reason (instance_change, record_expired).
	ColdStartsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Name:      "cold_starts_total",
			Help:      "Cold-start requests by reason",
		},
		[]string{"reason"},
	)

	// ContextTruncationsTotal tracks requests where client history was truncated.
	ContextTruncationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Name:      "context_truncations_total",
			Help:      "Requests whose client history was dropped or shrank",
		},
		[]string{"reason"},
	)

	// ContextOverflowTotal tracks requests rejected because the prompt exceeds
	// the model context window.
	ContextOverflowTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Name:      "context_overflow_total",
			Help:      "Context-window overflow events by stage",
		},
		[]string{"stage"},
	)

	// LLMMaxSeqLen exposes the effective --max-seq-len configured for the worker.
	LLMMaxSeqLen = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "koldun",
			Subsystem: "llm",
			Name:      "max_seq_len",
			Help:      "Effective maximum sequence length of the local model",
		},
	)

	// LLMWorkerReadForbiddenTotal tracks worker-load gate fail-opens caused by
	// missing RBAC on dllamas (observable misconfiguration).
	LLMWorkerReadForbiddenTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "llm",
			Name:      "worker_read_forbidden_total",
			Help:      "Times the llm sidecar could not read Dllama status due to RBAC",
		},
	)

	// LLMStreamAbortsTotal tracks interrupted upstream streams by phase.
	LLMStreamAbortsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "llm",
			Name:      "stream_aborts_total",
			Help:      "Interrupted upstream streams by phase (pre_token, post_token)",
		},
		[]string{"phase"},
	)

	// LLMDedupTotal tracks duplicate request suppressions.
	LLMDedupTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "llm",
			Name:      "dedup_total",
			Help:      "Duplicate assignment handling by result",
		},
		[]string{"result"},
	)

	// DispatcherAffinityEventsTotal tracks worker affinity decisions.
	DispatcherAffinityEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "dispatcher",
			Name:      "affinity_events_total",
			Help:      "Worker affinity decisions by result",
		},
		[]string{"result"},
	)

	// DispatcherCacheEvictionsTotal tracks pinned-worker invalidations.
	DispatcherCacheEvictionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "koldun",
			Subsystem: "dispatcher",
			Name:      "cache_evictions_total",
			Help:      "Pinned worker evictions by reason",
		},
		[]string{"reason"},
	)
)
