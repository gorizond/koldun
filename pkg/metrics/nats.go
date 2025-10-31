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
)
