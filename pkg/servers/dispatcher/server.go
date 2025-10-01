package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gorizond/koldun/pkg/conversation"
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
	Logger              *logrus.Entry
}

// Server coordinates backlog assignments for a single session.
type Server struct {
	cfg Config
	log *logrus.Entry

	nc *nats.Conn
	js nats.JetStreamContext

	backlogSub  *nats.Subscription
	stateSub    *nats.Subscription
	assignments nats.KeyValue

	mu       sync.Mutex
	workers  map[string]*workerState
	inflight map[string]*assignment
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

// New constructs a dispatcher instance.
func New(cfg Config) (*Server, error) {
	if strings.TrimSpace(cfg.Hash) == "" {
		return nil, fmt.Errorf("hash is required")
	}
	if strings.TrimSpace(cfg.NATSURL) == "" {
		return nil, fmt.Errorf("nats url is required")
	}
	if strings.TrimSpace(cfg.BacklogSubject) == "" {
		return nil, fmt.Errorf("backlog subject is required")
	}
	if strings.TrimSpace(cfg.AssignmentsBucket) == "" {
		return nil, fmt.Errorf("assignments bucket is required")
	}
	if strings.TrimSpace(cfg.DllamaSubjectPrefix) == "" {
		return nil, fmt.Errorf("dllama subject prefix is required")
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = 2 * time.Minute
	}
	cfg.DllamaSubjectPrefix = ensureTrailingDot(cfg.DllamaSubjectPrefix)
	if strings.TrimSpace(cfg.StateSubjectPrefix) == "" {
		cfg.StateSubjectPrefix = cfg.DllamaSubjectPrefix
	} else {
		cfg.StateSubjectPrefix = ensureTrailingDot(cfg.StateSubjectPrefix)
	}
	if cfg.QueueGroup == "" {
		cfg.QueueGroup = fmt.Sprintf("dispatcher-%s", sanitizeIdentifier(cfg.Hash))
	}

	logger := cfg.Logger
	if logger == nil {
		logger = logrus.StandardLogger().WithField("component", "dispatcher")
	}

	nc, err := nats.Connect(cfg.NATSURL, nats.Name(fmt.Sprintf("dispatcher-%s", cfg.Hash)))
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	kv, err := js.KeyValue(cfg.AssignmentsBucket)
	if err != nil {
		if errors.Is(err, nats.ErrBucketNotFound) {
			kv, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: cfg.AssignmentsBucket, History: 1})
		}
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("assignments bucket %s: %w", cfg.AssignmentsBucket, err)
		}
	}

	srv := &Server{
		cfg:         cfg,
		log:         logger,
		nc:          nc,
		js:          js,
		assignments: kv,
		workers:     make(map[string]*workerState),
		inflight:    make(map[string]*assignment),
	}
	return srv, nil
}

// Run starts the dispatcher loops until context cancellation.

func (s *Server) Run(ctx context.Context) error {
	defer s.nc.Drain()

	if err := s.startStateSubscription(); err != nil {
		return err
	}

	if err := s.startBacklogSubscription(); err != nil {
		return err
	}

	s.recoverAssignments()

	<-ctx.Done()
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
	subject := fmt.Sprintf("%s>.state", s.cfg.StateSubjectPrefix)
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

	worker := s.selectWorker()
	if worker == "" {
		s.log.Debug("dispatcher backlog: no idle workers available, requeuing")
		if err := s.nc.Publish(s.cfg.BacklogSubject, msg.Data); err != nil {
			s.log.WithError(err).Warn("requeue backlog message")
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
	if err := s.nc.Publish(subject, payload); err != nil {
		s.log.WithError(err).WithField("subject", subject).Warn("publish assignment")
		return
	}

	if _, err := s.assignments.Put(backlog.ID, payload); err != nil {
		s.log.WithError(err).Warn("record assignment kv")
	}

	s.mu.Lock()
	s.inflight[assignmentID] = &assignment{
		assignmentID: assignmentID,
		requestID:    backlog.ID,
		worker:       worker,
		payload:      backlog.Payload,
	}
	s.mu.Unlock()

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

	if err := s.assignments.Delete(asn.requestID); err != nil && !errors.Is(err, nats.ErrKeyNotFound) {
		s.log.WithError(err).WithField("requestID", asn.requestID).Warn("delete assignment kv")
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
	if err := s.nc.Publish(s.cfg.BacklogSubject, body); err != nil {
		s.log.WithError(err).Warn("republish backlog")
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
		if err := s.assignments.Delete(key); err != nil {
			s.log.WithError(err).WithField("key", key).Warn("cleanup assignment entry")
		}
	}
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
