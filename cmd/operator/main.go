package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gorizond/koldun/pkg/controllers"
	"github.com/gorizond/koldun/pkg/kube"
	"github.com/gorizond/koldun/pkg/servers/dispatcher"
	"github.com/gorizond/koldun/pkg/servers/ingress"
	"github.com/gorizond/koldun/pkg/servers/llm"
	operatorhealth "github.com/gorizond/koldun/pkg/servers/operator"
	"github.com/rancher/wrangler/v3/pkg/signals"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

type operatorManager interface {
	SetEnsureObjectStorageBuckets(bool)
	Register(context.Context) error
	Start(context.Context) error
	Health() *controllers.Health
}

type operatorHealthServer interface {
	Run(context.Context) error
}

type llmServer interface {
	Run(context.Context) error
}

type ingressServer interface {
	Run(context.Context) error
}

type dispatcherServer interface {
	Run(context.Context) error
}

var (
	buildConfigFn = kube.BuildConfig

	newOperatorManagerFn = func(cfg *rest.Config) (operatorManager, error) {
		return controllers.NewManager(cfg)
	}

	newHealthServerFn = func(cfg operatorhealth.Config) (operatorHealthServer, error) {
		return operatorhealth.New(cfg)
	}

	startRegistrySyncFn = func(ctx context.Context, mgr operatorManager, cfg controllers.RegistryConfig) error {
		typed, ok := mgr.(*controllers.Manager)
		if !ok {
			return fmt.Errorf("unsupported manager type %T", mgr)
		}
		return controllers.StartRegistrySync(ctx, typed, cfg)
	}

	startConversationReconcilerFn = func(ctx context.Context, mgr operatorManager, cfg controllers.ConversationConfig) error {
		typed, ok := mgr.(*controllers.Manager)
		if !ok {
			return fmt.Errorf("unsupported manager type %T", mgr)
		}
		return controllers.StartConversationReconciler(ctx, typed, cfg)
	}

	newLLMServerFn = func(cfg llm.Config) (llmServer, error) {
		return llm.New(cfg)
	}

	newIngressServerFn = func(cfg ingress.Config) (ingressServer, error) {
		return ingress.New(cfg)
	}

	newDispatcherServerFn = func(cfg dispatcher.Config) (dispatcherServer, error) {
		return dispatcher.New(cfg)
	}

	setupSignalContextFn = signals.SetupSignalContext
)

func main() {
	var (
		kubeconfig string
		mode       string

		// LLM flags
		llmListen                  string
		llmHash                    string
		llmNATSURL                 string
		llmInPrefix                string
		llmOutPrefix               string
		llmRequestSubject          string
		llmStateSubject            string
		llmDllamaName              string
		llmSidecarURL              string
		llmSidecarTimeout          time.Duration
		llmHealthOnly              bool
		llmNamespace               string
		llmSidecarMonitorInterval  time.Duration
		llmSidecarFailureThreshold int

		// Backend flags
		backendListen                         string
		backendNamespace                      string
		backendNATSURL                        string
		backendInPrefix                       string
		backendOutPrefix                      string
		backendTTLPrefix                      string
		backendConversationBucket             string
		backendModelsBucket                   string
		backendTokensBucket                   string
		backendModelPrefix                    string
		backendTokenPrefix                    string
		backendConversationTTL                time.Duration
		backendResponseTimeout                time.Duration
		backendSessionMinDllamas              int
		backendSessionMaxDllamas              int
		backendSessionScaleUpBacklog          int
		backendSessionScaleDownIdleSeconds    int
		backendSessionDispatcherImage         string
		backendSessionDispatcherMetricsListen string
		backendReplicaPower                   int
		backendHashSecret                     string
		backendAllowAnonymous                 bool
		backendRootImage                      string
		backendWorkerImage                    string

		// Dispatcher flags
		dispatcherHash              string
		dispatcherNATSURL           string
		dispatcherBacklogSubject    string
		dispatcherAssignmentsBucket string
		dispatcherDllamaPrefix      string
		dispatcherStatePrefix       string
		dispatcherQueueGroup        string
		dispatcherAckWait           time.Duration
		dispatcherMetricsListen     string

		// Operator conversation/registry flags
		operatorNATSURL             string
		operatorKVBucket            string
		operatorTTLPrefix           string
		operatorPollInterval        time.Duration
		operatorModelsBucket        string
		operatorTokensBucket        string
		operatorModelPrefix         string
		operatorTokenPrefix         string
		operatorHealthListen        string
		operatorDisableBucketEnsure bool
	)

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&mode, "mode", "operator", "Process mode: operator|llm|ingress (alias: backend)")
	fs.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig, falls back to in-cluster config")

	fs.StringVar(&llmListen, "llm-listen", ":8081", "LLM health endpoint listen address")
	fs.StringVar(&llmHash, "llm-hash", os.Getenv("HASH_KOLDUN"), "Conversation hash_koldun this worker handles")
	fs.StringVar(&llmNATSURL, "llm-nats-url", "nats://nats.default:4222", "NATS endpoint for LLM worker")
	fs.StringVar(&llmInPrefix, "llm-in-prefix", "in.", "NATS subject prefix for inbound messages (must end with '.')")
	fs.StringVar(&llmOutPrefix, "llm-out-prefix", "out.", "NATS subject prefix for outbound messages")
	fs.StringVar(&llmRequestSubject, "llm-request-subject", "", "Full NATS subject delivering dispatcher assignments to this worker")
	fs.StringVar(&llmStateSubject, "llm-state-subject", "", "NATS subject where worker publishes state heartbeats")
	fs.StringVar(&llmDllamaName, "llm-dllama-name", "", "Identifier of the dllama worker for dispatcher accounting")
	fs.StringVar(&llmSidecarURL, "llm-sidecar-url", "http://127.0.0.1:8080", "Base URL for dllama-api sidecar")
	fs.DurationVar(&llmSidecarTimeout, "llm-sidecar-timeout", 2*time.Minute, "Timeout for sidecar HTTP calls")
	fs.BoolVar(&llmHealthOnly, "llm-health-only", false, "Disable health server (useful for tests)")
	fs.StringVar(&llmNamespace, "llm-namespace", os.Getenv("POD_NAMESPACE"), "Namespace containing the dllama resource (defaults to pod namespace when available)")
	fs.DurationVar(&llmSidecarMonitorInterval, "llm-sidecar-monitor-interval", 15*time.Second, "Interval between dllama-api health probes")
	fs.IntVar(&llmSidecarFailureThreshold, "llm-sidecar-failure-threshold", 4, "Consecutive failed dllama-api probes before the worker evicts itself")

	fs.StringVar(&dispatcherHash, "dispatcher-hash", "", "Conversation hash this dispatcher serves")
	fs.StringVar(&dispatcherNATSURL, "dispatcher-nats-url", "nats://nats.default:4222", "NATS endpoint for dispatcher")
	fs.StringVar(&dispatcherBacklogSubject, "dispatcher-backlog-subject", "", "NATS subject holding backlog requests")
	fs.StringVar(&dispatcherAssignmentsBucket, "dispatcher-assignments-bucket", "", "JetStream KV bucket for assignment tracking")
	fs.StringVar(&dispatcherDllamaPrefix, "dispatcher-dllama-prefix", "", "Subject prefix used for worker assignments (must end with '.')")
	fs.StringVar(&dispatcherStatePrefix, "dispatcher-state-prefix", "", "Subject prefix used for worker state heartbeats (defaults to dllama prefix)")
	fs.StringVar(&dispatcherQueueGroup, "dispatcher-queue-group", "", "Queue group name for backlog consumption")
	fs.DurationVar(&dispatcherAckWait, "dispatcher-ack-wait", 2*time.Minute, "Ack wait for backlog messages")
	fs.StringVar(&dispatcherMetricsListen, "dispatcher-metrics-listen", "", "Listen address for dispatcher metrics and health endpoints (empty disables)")

	fs.StringVar(&backendListen, "backend-listen", ":8082", "Backend HTTP listen address")
	fs.StringVar(&backendNamespace, "backend-namespace", "default", "Namespace for Dllama resources")
	fs.StringVar(&backendNATSURL, "backend-nats-url", "nats://nats.default:4222", "NATS endpoint for backend")
	fs.StringVar(&backendInPrefix, "backend-in-prefix", "in.", "NATS subject prefix for inbound messages (must end with '.')")
	fs.StringVar(&backendOutPrefix, "backend-out-prefix", "out.", "NATS subject prefix for outbound messages")
	fs.StringVar(&backendTTLPrefix, "backend-ttl-prefix", "nats_ttl_", "Key prefix for conversation records")
	fs.StringVar(&backendConversationBucket, "backend-conversation-bucket", "koldun_ttl", "KeyValue bucket for conversation records")
	fs.StringVar(&backendModelsBucket, "backend-models-bucket", "koldun_models", "KeyValue bucket containing ready model metadata")
	fs.StringVar(&backendTokensBucket, "backend-tokens-bucket", "koldun_tokens", "KeyValue bucket containing API tokens")
	fs.StringVar(&backendModelPrefix, "backend-model-prefix", "model/", "Key prefix for model entries in the registry bucket")
	fs.StringVar(&backendTokenPrefix, "backend-token-prefix", "token/", "Key prefix for token entries in the registry bucket")
	fs.DurationVar(&backendConversationTTL, "backend-conversation-ttl", 10*time.Minute, "Conversation lifetime (JetStream TTL)")
	fs.DurationVar(&backendResponseTimeout, "backend-response-timeout", 2*time.Minute, "Timeout waiting for replies from NATS")
	fs.IntVar(&backendSessionMinDllamas, "backend-session-min-dllamas", 1, "Minimum number of Dllama resources per session")
	fs.IntVar(&backendSessionMaxDllamas, "backend-session-max-dllamas", 0, "Maximum number of Dllama resources per session (0 = unlimited)")
	fs.IntVar(&backendSessionScaleUpBacklog, "backend-session-scale-up-backlog", 0, "Queued message threshold to trigger additional Dllama instances")
	fs.IntVar(&backendSessionScaleDownIdleSeconds, "backend-session-scale-down-idle-seconds", 0, "Idle seconds before scaling down Dllama instances")
	fs.StringVar(&backendSessionDispatcherImage, "backend-session-dispatcher-image", "", "Container image for session dispatcher pods (defaults to backend image)")
	fs.StringVar(&backendSessionDispatcherMetricsListen, "backend-session-dispatcher-metrics-listen", "", "Listen address for session dispatcher metrics/health endpoints (empty disables)")
	fs.StringVar(&backendHashSecret, "backend-hash-secret", "", "Optional secret used for hash_koldun HMAC (base64/plain)")
	fs.BoolVar(&backendAllowAnonymous, "backend-allow-anonymous", false, "Allow ingress backend to accept requests without API tokens")
	fs.StringVar(&backendRootImage, "backend-root-image", "", "Container image for Dllama root pods")
	fs.StringVar(&backendWorkerImage, "backend-worker-image", "", "Container image for Dllama worker pods")
	fs.IntVar(&backendReplicaPower, "backend-replica-power", 0, "Override replica power for Sessions created by the backend (0 uses model setting)")

	fs.StringVar(&operatorNATSURL, "operator-nats-url", "", "NATS endpoint used by the operator to reconcile conversations and publish registry data")
	fs.StringVar(&operatorKVBucket, "operator-kv-bucket", "", "JetStream KeyValue bucket containing conversation records")
	fs.StringVar(&operatorTTLPrefix, "operator-ttl-prefix", "nats_ttl_", "Prefix for conversation TTL keys")
	fs.DurationVar(&operatorPollInterval, "operator-poll-interval", 10*time.Second, "Polling interval for operator conversation sync")
	fs.StringVar(&operatorModelsBucket, "operator-models-bucket", "", "JetStream KeyValue bucket where ready models are published (default koldun_models)")
	fs.StringVar(&operatorTokensBucket, "operator-tokens-bucket", "", "JetStream KeyValue bucket where API tokens are published (default koldun_tokens)")
	fs.StringVar(&operatorModelPrefix, "operator-model-prefix", "", "Key prefix for model entries in the registry bucket (default model/)")
	fs.StringVar(&operatorTokenPrefix, "operator-token-prefix", "", "Key prefix for token entries in the registry bucket (default token/)")
	fs.StringVar(&operatorHealthListen, "operator-health-listen", ":8080", "Operator health endpoint listen address")
	fs.BoolVar(&operatorDisableBucketEnsure, "operator-disable-bucket-ensure", false, "Skip automatic verification and creation of Model objectStorage buckets")

	klog.InitFlags(fs)

	args := os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		mode = args[0]
		args = args[1:]
	}

	if err := fs.Parse(args); err != nil {
		logrus.WithError(err).Fatal("parse flags")
	}

	hashSecret := decodeHashSecret(backendHashSecret)

	klog.SetOutput(os.Stderr)
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})

	ctx := setupSignalContextFn()

	switch strings.ToLower(mode) {
	case "operator":
		runOperator(ctx, kubeconfig,
			controllers.ConversationConfig{
				NATSURL:      operatorNATSURL,
				KVBucket:     operatorKVBucket,
				TTLPrefix:    operatorTTLPrefix,
				PollInterval: operatorPollInterval,
			},
			controllers.RegistryConfig{
				NATSURL:      operatorNATSURL,
				ModelsBucket: operatorModelsBucket,
				TokensBucket: operatorTokensBucket,
				ModelPrefix:  operatorModelPrefix,
				TokenPrefix:  operatorTokenPrefix,
			},
			operatorHealthListen,
			operatorDisableBucketEnsure,
		)
	case "llm":
		runLLM(ctx, llm.Config{
			Hash:                    llmHash,
			ListenAddress:           llmListen,
			NATSURL:                 llmNATSURL,
			InPrefix:                llmInPrefix,
			OutPrefix:               llmOutPrefix,
			RequestSubject:          llmRequestSubject,
			StateSubject:            llmStateSubject,
			DllamaName:              llmDllamaName,
			Namespace:               llmNamespace,
			SidecarURL:              llmSidecarURL,
			SidecarTimeout:          llmSidecarTimeout,
			SidecarMonitorInterval:  llmSidecarMonitorInterval,
			SidecarFailureThreshold: llmSidecarFailureThreshold,
			HealthOnly:              llmHealthOnly,
		})
	case "backend", "ingress":
		runIngress(ctx, ingress.Config{
			ListenAddress:                  backendListen,
			Namespace:                      backendNamespace,
			RootImage:                      backendRootImage,
			WorkerImage:                    backendWorkerImage,
			NATSURL:                        backendNATSURL,
			ConversationBucket:             backendConversationBucket,
			ModelsBucket:                   backendModelsBucket,
			TokensBucket:                   backendTokensBucket,
			InPrefix:                       backendInPrefix,
			OutPrefix:                      backendOutPrefix,
			TTLPrefix:                      backendTTLPrefix,
			ModelPrefix:                    backendModelPrefix,
			TokenPrefix:                    backendTokenPrefix,
			ConversationTTL:                backendConversationTTL,
			ResponseTimeout:                backendResponseTimeout,
			SessionMinDllamas:              int32(backendSessionMinDllamas),
			SessionMaxDllamas:              int32(backendSessionMaxDllamas),
			SessionScaleUpBacklog:          int32(backendSessionScaleUpBacklog),
			SessionScaleDownIdleSeconds:    int32(backendSessionScaleDownIdleSeconds),
			SessionDispatcherImage:         backendSessionDispatcherImage,
			SessionDispatcherMetricsListen: backendSessionDispatcherMetricsListen,
			HashSecret:                     hashSecret,
			AllowAnonymous:                 backendAllowAnonymous,
			ReplicaPower:                   int32(backendReplicaPower),
		})
	case "dispatcher":
		statePrefix := strings.TrimSpace(dispatcherStatePrefix)
		if statePrefix == "" {
			logrus.Fatal("dispatcher state subject prefix is required")
		}
		if !strings.HasSuffix(statePrefix, ".") {
			logrus.Fatal("dispatcher state subject prefix must end with '.'")
		}

		runDispatcher(ctx, dispatcher.Config{
			Hash:                dispatcherHash,
			NATSURL:             dispatcherNATSURL,
			BacklogSubject:      dispatcherBacklogSubject,
			AssignmentsBucket:   dispatcherAssignmentsBucket,
			DllamaSubjectPrefix: dispatcherDllamaPrefix,
			StateSubjectPrefix:  statePrefix,
			QueueGroup:          dispatcherQueueGroup,
			AckWait:             dispatcherAckWait,
			MetricsAddr:         dispatcherMetricsListen,
		})
	default:
		logrus.Fatalf("unknown mode %q", mode)
	}
}

func runOperator(ctx context.Context, kubeconfig string, convCfg controllers.ConversationConfig, registryCfg controllers.RegistryConfig, healthListen string, disableBucketEnsure bool) {
	cfg, err := buildConfigFn(kubeconfig)
	if err != nil {
		logrus.Fatalf("failed to build Kubernetes config: %v", err)
	}

	manager, err := newOperatorManagerFn(cfg)
	if err != nil {
		logrus.Fatalf("failed to create controller manager: %v", err)
	}
	manager.SetEnsureObjectStorageBuckets(!disableBucketEnsure)

	healthServer, err := newHealthServerFn(operatorhealth.Config{
		ListenAddress: healthListen,
		Health:        manager.Health(),
	})
	if err != nil {
		logrus.Fatalf("failed to create health server: %v", err)
	}

	if err := manager.Register(ctx); err != nil {
		logrus.Fatalf("failed to register controllers: %v", err)
	}

	if err := startRegistrySyncFn(ctx, manager, registryCfg); err != nil {
		logrus.Fatalf("failed to start registry sync: %v", err)
	}

	if err := startConversationReconcilerFn(ctx, manager, convCfg); err != nil {
		logrus.Fatalf("failed to start conversation reconciler: %v", err)
	}

	go func() {
		if err := healthServer.Run(ctx); err != nil {
			logrus.Fatalf("health server exited with error: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		manager.Health().SetAPIHealthy(false)
		manager.Health().SetCachesSynced(false)
	}()

	logrus.Info("starting koldun operator")
	klog.Info("koldun operator is starting up")
	if err := manager.Start(ctx); err != nil {
		klog.Errorf("controller manager exited with error: %v", err)
		logrus.Fatalf("controller manager exited with error: %v", err)
	}

	<-ctx.Done()
	logrus.Info("koldun operator context cancelled, shutting down")
}

func runLLM(ctx context.Context, cfg llm.Config) {
	server, err := newLLMServerFn(cfg)
	if err != nil {
		logrus.Fatalf("failed to initialise llm server: %v", err)
	}
	if err := server.Run(ctx); err != nil {
		logrus.Fatalf("llm server exited with error: %v", err)
	}
}

func runIngress(ctx context.Context, cfg ingress.Config) {
	server, err := newIngressServerFn(cfg)
	if err != nil {
		logrus.Fatalf("failed to initialise ingress server: %v", err)
	}
	if err := server.Run(ctx); err != nil {
		logrus.Fatalf("ingress server exited with error: %v", err)
	}
}

func runDispatcher(ctx context.Context, cfg dispatcher.Config) {
	server, err := newDispatcherServerFn(cfg)
	if err != nil {
		logrus.Fatalf("failed to initialise dispatcher: %v", err)
	}
	if err := server.Run(ctx); err != nil {
		logrus.Fatalf("dispatcher exited with error: %v", err)
	}
}

func decodeHashSecret(value string) []byte {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}

	base64Value, forced := trimBase64Prefix(trimmed)
	if base64Value == "" && strings.ContainsAny(trimmed, "+/=_-") {
		base64Value = trimmed
	}

	if base64Value != "" {
		if decoded, ok := tryDecodeBase64(base64Value); ok {
			return decoded
		}
		if forced {
			return []byte(base64Value)
		}
	}

	return []byte(trimmed)
}

func trimBase64Prefix(value string) (string, bool) {
	switch {
	case strings.HasPrefix(value, "base64:"):
		return value[len("base64:"):], true
	case strings.HasPrefix(value, "b64:"):
		return value[len("b64:"):], true
	default:
		return "", false
	}
}

func tryDecodeBase64(value string) ([]byte, bool) {
	encodings := []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	}
	for _, enc := range encodings {
		decoded, err := enc.DecodeString(value)
		if err != nil {
			continue
		}
		if enc.EncodeToString(decoded) == value {
			return decoded, true
		}
	}
	return nil, false
}
