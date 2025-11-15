package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/nats-io/nats.go"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	validation "k8s.io/apimachinery/pkg/util/validation"
)

// ConversationConfig configures the JetStream-backed reconciler that materialises
// Dllama resources for active conversations.
type ConversationConfig struct {
	NATSURL      string
	KVBucket     string
	TTLPrefix    string
	PollInterval time.Duration

	dialer natsDialer
}

type natsConnection interface {
	JetStream(opts ...nats.JSOpt) (nats.JetStreamContext, error)
	Close()
	Drain() error
}

type natsDialer func(url string, opts ...nats.Option) (natsConnection, error)

type keyValueProvider interface {
	KeyValue(bucket string) (nats.KeyValue, error)
	CreateKeyValue(cfg *nats.KeyValueConfig) (nats.KeyValue, error)
}

type conversationReconciler struct {
	cfg ConversationConfig

	log      *logrus.Entry
	sessions generic.ControllerInterface[*v1.Session, *v1.SessionList]
	apply    apply.Apply

	dialer   natsDialer
	dialOpts []nats.Option

	conn natsConnection
	kv   nats.KeyValue

	reconnectFn func(context.Context) error
}

// StartConversationReconciler initialises the conversation watcher if the NATS
// configuration is provided. When disabled (no NATS URL), the operator simply
// logs and returns nil.
func StartConversationReconciler(ctx context.Context, m *Manager, cfg ConversationConfig) error {
	if strings.TrimSpace(cfg.NATSURL) == "" {
		logrus.Info("conversation reconciler disabled: operator-nats-url not set")
		return nil
	}
	if strings.TrimSpace(cfg.KVBucket) == "" {
		return fmt.Errorf("conversation reconciler requires operator-kv-bucket")
	}
	if cfg.TTLPrefix == "" {
		cfg.TTLPrefix = "nats_ttl_"
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 10 * time.Second
	}

	log := logrus.StandardLogger().WithField("component", "conversation-reconciler")

	dial := cfg.dialer
	if dial == nil {
		dial = func(url string, opts ...nats.Option) (natsConnection, error) {
			return nats.Connect(url, opts...)
		}
	}

	reconciler := &conversationReconciler{
		cfg:      cfg,
		log:      log,
		sessions: m.Kold.Session(),
		apply:    m.Apply(ctx),
		dialer:   dial,
		dialOpts: []nats.Option{nats.Name("koldun-operator")},
	}
	reconciler.reconnectFn = reconciler.reconnect

	if err := reconciler.openConnection(); err != nil {
		return err
	}

	go reconciler.run(ctx)
	return nil
}

func ensureConversationBucket(js keyValueProvider, bucket string) (nats.KeyValue, error) {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return nil, fmt.Errorf("bucket name cannot be empty")
	}

	kv, err := js.KeyValue(bucket)
	if err == nil {
		return kv, nil
	}
	if errors.Is(err, nats.ErrBucketNotFound) {
		return js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket, History: 1})
	}
	return nil, err
}

func (r *conversationReconciler) run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	defer r.drainConn()

	if !r.syncWithReconnect(ctx) {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.syncWithReconnect(ctx) {
				return
			}
		}
	}
}

func (r *conversationReconciler) syncWithReconnect(ctx context.Context) bool {
	for {
		if ctx.Err() != nil {
			return false
		}

		if err := r.sync(); err != nil {
			if !isConnectionClosed(err) && !isBucketMissing(err) {
				r.log.WithError(err).Warn("sync conversations")
				return true
			}

			if isBucketMissing(err) {
				r.log.WithError(err).Warn("conversation bucket missing; reconnecting")
			} else {
				r.log.WithError(err).Warn("nats connection closed; reconnecting")
			}
			if err := r.reconnectWithOverride(ctx); err != nil {
				if ctx.Err() != nil {
					return false
				}
				r.log.WithError(err).Error("reconnect failed")
				return false
			}
			r.log.Info("conversation reconciler reconnected to NATS")
			continue
		}

		return true
	}
}

func (r *conversationReconciler) sync() error {
	keys, err := r.kv.Keys()
	if err == nats.ErrNoKeysFound {
		keys = nil
	} else if err != nil {
		if isConnectionClosed(err) || isBucketMissing(err) {
			return err
		}
		r.log.WithError(err).Warn("list kv keys")
		return nil
	}

	expected := make(map[string]struct{})
	for _, key := range keys {
		if !strings.HasPrefix(key, r.cfg.TTLPrefix) {
			continue
		}
		entry, err := r.kv.Get(key)
		if err != nil {
			if isConnectionClosed(err) || isBucketMissing(err) {
				return err
			}
			if err != nats.ErrKeyNotFound {
				r.log.WithError(err).WithField("key", key).Warn("get kv entry")
			}
			continue
		}
		record, err := conversation.ParseRecord(entry.Value())
		if err != nil {
			r.log.WithError(err).WithField("key", key).Warn("invalid conversation record")
			continue
		}

		if err := r.ensureSession(record); err != nil {
			r.log.WithError(err).WithField("hash", record.Hash).Error("ensure session")
		}

		expected[record.SessionNamespacedName()] = struct{}{}
	}

	existing, err := r.sessions.Cache().List("", labels.Everything())
	if err != nil {
		r.log.WithError(err).Warn("list sessions")
		return nil
	}

	for _, session := range existing {
		hash := strings.TrimSpace(session.Spec.Hash)
		if hash == "" {
			hash = labelValue(session.Labels, labelConversationHash)
		}
		if hash == "" {
			continue
		}

		key := fmt.Sprintf("%s/%s", session.Namespace, session.Name)
		if _, ok := expected[key]; ok {
			continue
		}

		if err := r.sessions.Delete(session.Namespace, session.Name, &metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			r.log.WithError(err).WithField("session", key).Error("delete stale session")
		}
	}

	return nil
}

func (r *conversationReconciler) ensureSession(record *conversation.Record) error {
	if strings.TrimSpace(record.SessionName()) == "" {
		return fmt.Errorf("session name missing for hash %s", record.Hash)
	}
	modelNamespace, modelName := record.ModelParts()
	if strings.TrimSpace(modelName) == "" {
		return fmt.Errorf("model name missing for hash %s", record.Hash)
	}
	if strings.TrimSpace(record.RootImage) == "" || strings.TrimSpace(record.WorkerImage) == "" {
		return fmt.Errorf("images missing for hash %s", record.Hash)
	}

	hashLabelValue := truncateName(record.Hash, validation.LabelValueMaxLength)
	labels := map[string]string{
		labelConversationHash: hashLabelValue,
	}

	session := &v1.Session{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       "Session",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      record.SessionName(),
			Namespace: record.Namespace,
			Labels:    labels,
		},
		Spec: v1.SessionSpec{
			Hash: record.Hash,
			ModelRef: v1.ModelReference{
				APIGroup: v1.GroupName,
				Kind:     "Model",
				Name:     modelName,
			},
			ReplicaPower:            record.ReplicaPower,
			RootImage:               record.RootImage,
			WorkerImage:             record.WorkerImage,
			DispatcherImage:         record.DispatcherImage,
			DispatcherMetricsListen: record.DispatcherMetricsListen,
			MinIdle: func() int32 {
				if record.Scaling != nil && record.Scaling.MinDllamas > 0 {
					return record.Scaling.MinDllamas
				}
				return 1
			}(),
			MaxWorkers: func() int32 {
				if record.Scaling != nil && record.Scaling.MaxDllamas > 0 {
					return record.Scaling.MaxDllamas
				}
				return 0
			}(),
			Scaling: func() *v1.SessionScalingSpec {
				if record.Scaling == nil {
					return nil
				}
				return &v1.SessionScalingSpec{
					MinDllamas:           record.Scaling.MinDllamas,
					MaxDllamas:           record.Scaling.MaxDllamas,
					ScaleUpBacklog:       record.Scaling.ScaleUpBacklog,
					ScaleDownIdleSeconds: record.Scaling.ScaleDownIdleSeconds,
					DesiredDllamas:       record.Scaling.DesiredDllamas,
				}
			}(),
			Queue: func() *v1.SessionQueueSpec {
				if record.Queue == nil {
					return nil
				}
				return &v1.SessionQueueSpec{
					BacklogSubject:        record.Queue.BacklogSubject,
					ResponseSubjectPrefix: record.Queue.ResponseSubjectPrefix,
					AssignmentsBucket:     record.Queue.AssignmentsBucket,
					DllamaSubjectPrefix:   record.Queue.DllamaSubjectPrefix,
					StateStream:           record.Queue.StateStream,
				}
			}(),
			NATS: func() *v1.SessionNATSConfig {
				if strings.TrimSpace(record.NATS.URL) == "" {
					return nil
				}
				cfg := &v1.SessionNATSConfig{URL: record.NATS.URL}
				if secret := strings.TrimSpace(record.NATS.CredentialsSecret); secret != "" {
					cfg.CredentialsSecret = &v1.SecretReference{Name: secret}
				}
				return cfg
			}(),
		},
	}

	if modelNamespace != "" && modelNamespace != record.Namespace {
		session.Spec.ModelRef.Namespace = modelNamespace
	}
	if session.Spec.ReplicaPower <= 0 {
		session.Spec.ReplicaPower = 1
	}

	return r.apply.WithDefaultNamespace(record.Namespace).
		WithSetID(fmt.Sprintf("conversation-session-%s", record.Hash)).
		ApplyObjects(session)
}

func (r *conversationReconciler) openConnection() error {
	if r.dialer == nil {
		return fmt.Errorf("conversation reconciler missing dialer")
	}

	r.closeConn()

	conn, err := r.dialer(r.cfg.NATSURL, r.dialOpts...)
	if err != nil {
		return fmt.Errorf("connect NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return fmt.Errorf("jetstream context: %w", err)
	}

	kv, err := ensureConversationBucket(js, r.cfg.KVBucket)
	if err != nil {
		conn.Close()
		return fmt.Errorf("kv bucket %s: %w", r.cfg.KVBucket, err)
	}

	r.conn = conn
	r.kv = kv
	return nil
}

func (r *conversationReconciler) reconnect(ctx context.Context) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := r.openConnection(); err == nil {
			return nil
		} else {
			r.log.WithError(err).Warn("failed to reconnect to NATS, will retry")
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (r *conversationReconciler) closeConn() {
	if r.conn != nil {
		r.conn.Close()
	}
	r.conn = nil
	r.kv = nil
}

func (r *conversationReconciler) drainConn() {
	if r.conn == nil {
		return
	}
	if err := r.conn.Drain(); err != nil {
		r.conn.Close()
	}
}

func (r *conversationReconciler) reconnectWithOverride(ctx context.Context) error {
	if r.reconnectFn != nil {
		return r.reconnectFn(ctx)
	}
	return r.reconnect(ctx)
}

func isConnectionClosed(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, nats.ErrConnectionClosed) ||
		errors.Is(err, nats.ErrConnectionDraining)
}

func isBucketMissing(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, nats.ErrBucketNotFound) ||
		errors.Is(err, nats.ErrStreamNotFound)
}
