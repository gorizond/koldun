package controllers

import (
	"context"
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
}

type conversationReconciler struct {
	cfg ConversationConfig

	log     *logrus.Entry
	dllamas generic.ControllerInterface[*v1.Dllama, *v1.DllamaList]
	apply   apply.Apply

	conn *nats.Conn
	kv   nats.KeyValue
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

	conn, err := nats.Connect(cfg.NATSURL, nats.Name("koldun-operator"))
	if err != nil {
		return fmt.Errorf("connect NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return fmt.Errorf("jetstream context: %w", err)
	}

	kv, err := js.KeyValue(cfg.KVBucket)
	if err != nil {
		conn.Close()
		return fmt.Errorf("kv bucket %s: %w", cfg.KVBucket, err)
	}

	reconciler := &conversationReconciler{
		cfg:     cfg,
		log:     log,
		dllamas: m.Kold.Dllama(),
		apply:   m.Apply(ctx),
		conn:    conn,
		kv:      kv,
	}

	go reconciler.run(ctx)
	return nil
}

func (r *conversationReconciler) run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	defer r.conn.Drain()

	r.sync(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sync(ctx)
		}
	}
}

func (r *conversationReconciler) sync(ctx context.Context) {
	keys, err := r.kv.Keys()
	if err == nats.ErrNoKeysFound {
		keys = nil
	} else if err != nil {
		r.log.WithError(err).Warn("list kv keys")
		return
	}

	expected := make(map[string]struct{})
	for _, key := range keys {
		if !strings.HasPrefix(key, r.cfg.TTLPrefix) {
			continue
		}
		entry, err := r.kv.Get(key)
		if err != nil {
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

		if err := r.ensureDllama(record); err != nil {
			r.log.WithError(err).WithField("hash", record.Hash).Error("ensure dllama")
		}

		expected[record.NamespacedName()] = struct{}{}
	}

	existing, err := r.dllamas.Cache().List("", labels.Everything())
	if err != nil {
		r.log.WithError(err).Warn("list dllamas")
		return
	}

	for _, dllama := range existing {
		hash := labelValue(dllama.Labels, labelConversationHash)
		if hash == "" {
			hash = labelValue(dllama.Annotations, labelConversationHash)
		}
		if hash == "" {
			continue
		}

		key := fmt.Sprintf("%s/%s", dllama.Namespace, dllama.Name)
		if _, ok := expected[key]; ok {
			continue
		}

		if err := r.dllamas.Delete(dllama.Namespace, dllama.Name, &metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			r.log.WithError(err).WithField("dllama", key).Error("delete stale dllama")
		}
	}
}

func (r *conversationReconciler) ensureDllama(record *conversation.Record) error {
	if strings.TrimSpace(record.Dllama) == "" {
		return fmt.Errorf("dllama name missing for hash %s", record.Hash)
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
		labelDllamaName:       record.Dllama,
		labelModelName:        modelName,
	}

	dllama := &v1.Dllama{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       "Dllama",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      record.Dllama,
			Namespace: record.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				labelConversationHash: record.Hash,
			},
		},
		Spec: v1.DllamaSpec{
			ModelRef: v1.ModelReference{
				APIGroup: v1.GroupName,
				Kind:     "Model",
				Name:     modelName,
			},
			ReplicaPower: record.ReplicaPower,
			RootImage:    record.RootImage,
			WorkerImage:  record.WorkerImage,
			NATS: func() *v1.DllamaNATSConfig {
				cfg := &v1.DllamaNATSConfig{URL: record.NATS.URL}
				if secret := strings.TrimSpace(record.NATS.CredentialsSecret); secret != "" {
					cfg.CredentialsSecret = &v1.SecretReference{Name: secret}
				}
				return cfg
			}(),
		},
	}

	if modelNamespace != "" && modelNamespace != record.Namespace {
		dllama.Spec.ModelRef.Namespace = modelNamespace
	}
	if dllama.Spec.ReplicaPower <= 0 {
		dllama.Spec.ReplicaPower = 1
	}

	return r.apply.WithDefaultNamespace(record.Namespace).
		WithSetID(fmt.Sprintf("conversation-%s", record.Hash)).
		ApplyObjects(dllama)
}
