package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/metrics"
	"github.com/gorizond/koldun/pkg/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// Config defines the runtime configuration for a session dispatcher.
type Config struct {
	Hash                string
	NATSURL             string
	BacklogSubject      string
	AssignmentsBucket   string
	DllamaSubjectPrefix string
	StateSubjectPrefix  string
	QueueGroup          string
	AckWait             time.Duration
	MetricsAddr         string // Address for metrics HTTP server (e.g., ":9090")
	Logger              *logrus.Entry
}

// Server coordinates backlog assignments for a single session.
type Server struct {
	cfg Config
	log *logrus.Entry

	nc          natsutil.NATSConn
	js          nats.JetStreamContext
	backlogSub  backlogSubscription
	stateSub    *nats.Subscription
	assignments natsutil.NATSKeyValue

	mu            sync.Mutex
	workers       map[string]*workerState
	inflight      map[string]*assignment
	lastNoIdleLog time.Time

	retryConfig   natsutil.RetryConfig
	metricsServer *http.Server
}

type backlogSubscription interface {
	Pending() (int, int, error)
}

type metricsTicker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct {
	*time.Ticker
}

func (t *realTicker) C() <-chan time.Time {
	return t.Ticker.C
}

var metricsTickerFactory = func(d time.Duration) metricsTicker {
	return &realTicker{Ticker: time.NewTicker(d)}
}

type workerState struct {
	name          string
	state         string
	active        int32
	lastHeartbeat time.Time
}

type assignment struct {
	assignmentID string
	requestID    string
	worker       string
	payload      []byte
}

var ErrQueueMisconfigured = errors.New("dispatcher queue misconfigured")

// New constructs a dispatcher instance.
func New(cfg Config) (*Server, error) {
	cfg.BacklogSubject = strings.TrimSpace(cfg.BacklogSubject)
	cfg.AssignmentsBucket = strings.TrimSpace(cfg.AssignmentsBucket)

	if strings.TrimSpace(cfg.Hash) == "" {
		return nil, fmt.Errorf("hash is required")
	}
	if strings.TrimSpace(cfg.NATSURL) == "" {
		return nil, fmt.Errorf("nats url is required")
	}
	if err := validateQueueConfig(cfg); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.DllamaSubjectPrefix) == "" {
		return nil, fmt.Errorf("dllama subject prefix is required")
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = 2 * time.Minute
	}
	cfg.DllamaSubjectPrefix = ensureTrailingDot(cfg.DllamaSubjectPrefix)
	statePrefix, err := sanitizeStateSubjectPrefix(cfg.StateSubjectPrefix)
	if err != nil {
		return nil, err
	}
	cfg.StateSubjectPrefix = statePrefix
	if cfg.QueueGroup == "" {
		cfg.QueueGroup = fmt.Sprintf("dispatcher-%s", sanitizeIdentifier(cfg.Hash))
	}

	logger := cfg.Logger
	if logger == nil {
		logger = logrus.StandardLogger().WithField("component", "dispatcher")
	}

	natsConn, err := nats.Connect(cfg.NATSURL, nats.Name(fmt.Sprintf("dispatcher-%s", cfg.Hash)))
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	js, err := natsConn.JetStream()
	if err != nil {
		natsConn.Close()
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	kvRaw, err := js.KeyValue(cfg.AssignmentsBucket)
	if err != nil {
		if errors.Is(err, nats.ErrBucketNotFound) {
			kvRaw, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: cfg.AssignmentsBucket, History: 1})
		}
		if err != nil {
			natsConn.Close()
			return nil, fmt.Errorf("assignments bucket %s: %w", cfg.AssignmentsBucket, err)
		}
	}

	// Wrap NATS objects in interfaces for testability
	nc := natsutil.NewNATSConnWrapper(natsConn)
	kv := natsutil.NewNATSKeyValueWrapper(kvRaw)

	srv := &Server{
		cfg:         cfg,
		log:         logger,
		nc:          nc,
		js:          js,
		assignments: kv,
		workers:     make(map[string]*workerState),
		inflight:    make(map[string]*assignment),
		retryConfig: natsutil.DefaultRetryConfig(),
	}
	return srv, nil
}

// Run starts the dispatcher loops until context cancellation.

func (s *Server) Run(ctx context.Context) error {
	defer s.nc.Drain()

	// Start metrics server if configured
	if s.cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		})

		s.metricsServer = &http.Server{
			Addr:    s.cfg.MetricsAddr,
			Handler: mux,
		}

		go func() {
			s.log.Infof("metrics server listening on %s", s.cfg.MetricsAddr)
			if err := s.metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				s.log.WithError(err).Error("metrics server error")
			}
		}()

		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := s.metricsServer.Shutdown(shutdownCtx); err != nil {
				s.log.WithError(err).Warn("metrics server shutdown error")
			}
		}()
	}

	// Track connection status
	metrics.NATSConnectionStatus.WithLabelValues(s.cfg.NATSURL).Set(1)
	defer metrics.NATSConnectionStatus.WithLabelValues(s.cfg.NATSURL).Set(0)

	if err := s.startStateSubscription(); err != nil {
		return err
	}

	if err := s.startBacklogSubscription(); err != nil {
		return err
	}

	s.recoverAssignments()

	// Start metrics update goroutine
	go s.updateMetricsPeriodically(ctx)

	<-ctx.Done()
	return nil
}

func validateQueueConfig(cfg Config) error {
	if cfg.BacklogSubject == "" {
		return fmt.Errorf("%w: backlog subject is required", ErrQueueMisconfigured)
	}
	if cfg.AssignmentsBucket == "" {
		return fmt.Errorf("%w: assignments bucket is required", ErrQueueMisconfigured)
	}
	return nil
}

func (s *Server) startBacklogSubscription() error {
	sub, err := s.nc.QueueSubscribe(s.cfg.BacklogSubject, s.cfg.QueueGroup, s.handleBacklog)
	if err != nil {
		return fmt.Errorf("subscribe backlog: %w", err)
	}
	s.backlogSub = sub
	return nil
}

func (s *Server) startStateSubscription() error {
	subject := fmt.Sprintf("%s*.state", s.cfg.StateSubjectPrefix)
	sub, err := s.nc.Subscribe(subject, s.handleState)
	if err != nil {
		return fmt.Errorf("subscribe state: %w", err)
	}
	s.stateSub = sub
	return nil
}

func (s *Server) handleBacklog(msg *nats.Msg) {
	if msg == nil {
		return
	}

	var backlog conversation.BacklogMessage
	if err := json.Unmarshal(msg.Data, &backlog); err != nil {
		s.log.WithError(err).Warn("invalid backlog payload")
		return
	}
	if len(backlog.Payload) == 0 {
		s.log.Warn("backlog missing payload")
		return
	}

	idleWorkers, totalWorkers, inflight := s.snapshotWorkerStats()

	worker := s.selectWorker()
	if worker == "" {
		if s.shouldLogNoIdle(time.Now()) {
			s.log.WithFields(logrus.Fields{
				"requestId":    backlog.ID,
				"idleWorkers":  idleWorkers,
				"totalWorkers": totalWorkers,
				"inflight":     inflight,
			}).Info("dispatcher backlog: no idle workers available, requeuing")
		}
		if err := PublishWithRetry(s.nc, s.cfg.BacklogSubject, msg.Data, s.retryConfig, s.log); err != nil {
			s.log.WithError(err).Error("failed to requeue backlog message after retries")
		}
		return
	}

	assignmentID := newAssignmentID()
	env := conversation.AssignmentEnvelope{
		AssignmentID: assignmentID,
		RequestID:    backlog.ID,
		Payload:      backlog.Payload,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		s.log.WithError(err).Warn("marshal assignment envelope")
		return
	}

	subject := fmt.Sprintf("%s%s.in", s.cfg.DllamaSubjectPrefix, worker)
	if err := PublishWithRetry(s.nc, subject, payload, s.retryConfig, s.log); err != nil {
		s.log.WithError(err).WithField("subject", subject).Error("failed to publish assignment after retries")
		// Requeue the backlog item since we couldn't dispatch it
		if requeueErr := PublishWithRetry(s.nc, s.cfg.BacklogSubject, msg.Data, s.retryConfig, s.log); requeueErr != nil {
			s.log.WithError(requeueErr).Error("failed to requeue after publish failure")
		}
		return
	}

	if _, err := KVPutWithRetry(s.assignments, backlog.ID, payload, s.retryConfig, s.log); err != nil {
		s.log.WithError(err).Error("failed to record assignment in KV after retries")
		// Continue execution - assignment was sent, KV is for recovery only
	}

	s.mu.Lock()
	s.inflight[assignmentID] = &assignment{
		assignmentID: assignmentID,
		requestID:    backlog.ID,
		worker:       worker,
		payload:      backlog.Payload,
	}
	s.mu.Unlock()

	nowIdle, total, nowInflight := s.snapshotWorkerStats()

	s.log.WithFields(logrus.Fields{
		"assignmentId": assignmentID,
		"requestId":    backlog.ID,
		"worker":       worker,
		"idleWorkers":  nowIdle,
		"totalWorkers": total,
		"inflight":     nowInflight,
	}).Info("dispatcher dispatched assignment")
}

func (s *Server) handleState(msg *nats.Msg) {
	if msg == nil {
		return
	}
	var event conversation.WorkerStateEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		s.log.WithError(err).Warn("invalid state event")
		return
	}
	worker := strings.TrimSpace(event.Dllama)
	if worker == "" {
		return
	}

	s.mu.Lock()
	st := s.workers[worker]
	if st == nil {
		st = &workerState{name: worker}
		s.workers[worker] = st
	}
	if event.Active >= 0 {
		st.active = event.Active
	}
	st.state = event.State
	st.lastHeartbeat = time.Now()
	assignmentID := event.AssignmentID
	s.mu.Unlock()

	switch strings.ToLower(event.State) {
	case "idle":
		if assignmentID != "" {
			s.finishAssignment(assignmentID, false)
		}
	case "error":
		if assignmentID != "" {
			s.finishAssignment(assignmentID, true)
		}
	}
}

func (s *Server) finishAssignment(assignmentID string, requeue bool) {
	s.mu.Lock()
	asn, ok := s.inflight[assignmentID]
	if ok {
		delete(s.inflight, assignmentID)
	}
	s.mu.Unlock()
	if !ok {
		return
	}

	if requeue {
		s.requeueAssignment(asn)
	}

	if err := KVDeleteWithRetry(s.assignments, asn.requestID, s.retryConfig, s.log); err != nil {
		s.log.WithError(err).WithField("requestID", asn.requestID).Error("failed to delete assignment from KV after retries")
		// Continue - this is cleanup, not critical
	}
}

func (s *Server) requeueAssignment(asn *assignment) {
	backlog := conversation.BacklogMessage{
		ID:        asn.requestID,
		Payload:   asn.payload,
		CreatedAt: time.Now().Unix(),
	}
	body, err := json.Marshal(backlog)
	if err != nil {
		s.log.WithError(err).Warn("marshal backlog retry")
		return
	}
	if err := PublishWithRetry(s.nc, s.cfg.BacklogSubject, body, s.retryConfig, s.log); err != nil {
		s.log.WithError(err).Error("failed to republish backlog after retries")
	}
}

func (s *Server) selectWorker() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var selected string
	var selectedTime time.Time
	for name, st := range s.workers {
		if strings.ToLower(st.state) != "idle" {
			continue
		}
		if st.active > 0 {
			continue
		}
		if selected == "" || st.lastHeartbeat.Before(selectedTime) {
			selected = name
			selectedTime = st.lastHeartbeat
		}
	}
	return selected
}

func (s *Server) recoverAssignments() {
	keys, err := s.assignments.Keys()
	if err != nil {
		if errors.Is(err, nats.ErrNoKeysFound) {
			return
		}
		s.log.WithError(err).Warn("list assignments")
		return
	}
	for _, key := range keys {
		entry, err := s.assignments.Get(key)
		if err != nil {
			s.log.WithError(err).WithField("key", key).Warn("get assignment entry")
			continue
		}
		var env conversation.AssignmentEnvelope
		if err := json.Unmarshal(entry.Value(), &env); err != nil {
			s.log.WithError(err).WithField("key", key).Warn("parse assignment entry")
			continue
		}
		s.requeueAssignment(&assignment{
			assignmentID: env.AssignmentID,
			requestID:    env.RequestID,
			payload:      env.Payload,
		})
		if err := KVDeleteWithRetry(s.assignments, key, s.retryConfig, s.log); err != nil {
			s.log.WithError(err).WithField("key", key).Error("failed to cleanup assignment entry after retries")
		}
	}
}

func (s *Server) snapshotWorkerStats() (idle int, total int, inflight int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.workers {
		total++
		if strings.ToLower(st.state) == "idle" && st.active == 0 {
			idle++
		}
	}
	inflight = len(s.inflight)
	return
}

func (s *Server) shouldLogNoIdle(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.Sub(s.lastNoIdleLog) < 5*time.Second {
		return false
	}
	s.lastNoIdleLog = now
	return true
}

func sanitizeStateSubjectPrefix(prefix string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "", fmt.Errorf("state subject prefix is required")
	}
	return ensureTrailingDot(prefix), nil
}

func ensureTrailingDot(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	if strings.HasSuffix(prefix, ".") {
		return prefix
	}
	return prefix + "."
}

func sanitizeIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return value
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

func newAssignmentID() string {
	return fmt.Sprintf("assign-%d", time.Now().UnixNano())
}

func (s *Server) updateMetricsPeriodically(ctx context.Context) {
	ticker := metricsTickerFactory(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			s.updateMetrics()
		}
	}
}

func (s *Server) updateMetrics() {
	s.mu.Lock()
	activeWorkers := 0
	for _, st := range s.workers {
		if strings.ToLower(st.state) == "idle" && st.active == 0 {
			activeWorkers++
		}
	}
	inflightCount := len(s.inflight)
	s.mu.Unlock()

	metrics.DispatcherWorkersActive.Set(float64(activeWorkers))
	metrics.DispatcherAssignmentsInflight.Set(float64(inflightCount))

	// Update backlog size if possible
	if s.backlogSub != nil {
		if pending, _, err := s.backlogSub.Pending(); err == nil {
			metrics.DispatcherBacklogSize.Set(float64(pending))
		}
	}
}
