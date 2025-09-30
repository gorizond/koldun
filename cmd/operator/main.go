package main

import (
	"context"
	"flag"
	"os"
	"strings"
	"time"

	"github.com/gorizond/koldun/pkg/controllers"
	"github.com/gorizond/koldun/pkg/kube"
	"github.com/gorizond/koldun/pkg/servers/ingress"
	"github.com/gorizond/koldun/pkg/servers/llm"
	"github.com/rancher/wrangler/v3/pkg/signals"
	"github.com/sirupsen/logrus"
	"k8s.io/klog/v2"
)

func main() {
	var (
		kubeconfig string
		mode       string

		// LLM flags
		llmListen         string
		llmHash           string
		llmNATSURL        string
		llmInPrefix       string
		llmOutPrefix      string
		llmSidecarURL     string
		llmSidecarTimeout time.Duration
		llmHealthOnly     bool

		// Backend flags
		backendListen             string
		backendNamespace          string
		backendNATSURL            string
		backendInPrefix           string
		backendOutPrefix          string
		backendTTLPrefix          string
		backendConversationBucket string
		backendModelsBucket       string
		backendTokensBucket       string
		backendModelPrefix        string
		backendTokenPrefix        string
		backendConversationTTL    time.Duration
		backendResponseTimeout    time.Duration
		backendHashSecret         string
		backendRootImage          string
		backendWorkerImage        string

		// Operator conversation/registry flags
		operatorNATSURL      string
		operatorKVBucket     string
		operatorTTLPrefix    string
		operatorPollInterval time.Duration
		operatorModelsBucket string
		operatorTokensBucket string
		operatorModelPrefix  string
		operatorTokenPrefix  string
	)

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&mode, "mode", "operator", "Process mode: operator|llm|ingress (alias: backend)")
	fs.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig, falls back to in-cluster config")

	fs.StringVar(&llmListen, "llm-listen", ":8081", "LLM health endpoint listen address")
	fs.StringVar(&llmHash, "llm-hash", os.Getenv("HASH_KOLDUN"), "Conversation hash_koldun this worker handles")
	fs.StringVar(&llmNATSURL, "llm-nats-url", "nats://nats.default:4222", "NATS endpoint for LLM worker")
	fs.StringVar(&llmInPrefix, "llm-in-prefix", "in_", "NATS subject prefix for inbound messages")
	fs.StringVar(&llmOutPrefix, "llm-out-prefix", "out_", "NATS subject prefix for outbound messages")
	fs.StringVar(&llmSidecarURL, "llm-sidecar-url", "http://127.0.0.1:8080", "Base URL for dllama-api sidecar")
	fs.DurationVar(&llmSidecarTimeout, "llm-sidecar-timeout", 2*time.Minute, "Timeout for sidecar HTTP calls")
	fs.BoolVar(&llmHealthOnly, "llm-health-only", false, "Disable health server (useful for tests)")

	fs.StringVar(&backendListen, "backend-listen", ":8082", "Backend HTTP listen address")
	fs.StringVar(&backendNamespace, "backend-namespace", "default", "Namespace for Dllama resources")
	fs.StringVar(&backendNATSURL, "backend-nats-url", "nats://nats.default:4222", "NATS endpoint for backend")
	fs.StringVar(&backendInPrefix, "backend-in-prefix", "in_", "NATS subject prefix for inbound messages")
	fs.StringVar(&backendOutPrefix, "backend-out-prefix", "out_", "NATS subject prefix for outbound messages")
	fs.StringVar(&backendTTLPrefix, "backend-ttl-prefix", "nats_ttl_", "Key prefix for conversation records")
	fs.StringVar(&backendConversationBucket, "backend-conversation-bucket", "koldun_ttl", "KeyValue bucket for conversation records")
	fs.StringVar(&backendModelsBucket, "backend-models-bucket", "koldun_models", "KeyValue bucket containing ready model metadata")
	fs.StringVar(&backendTokensBucket, "backend-tokens-bucket", "koldun_tokens", "KeyValue bucket containing API tokens")
	fs.StringVar(&backendModelPrefix, "backend-model-prefix", "model/", "Key prefix for model entries in the registry bucket")
	fs.StringVar(&backendTokenPrefix, "backend-token-prefix", "token/", "Key prefix for token entries in the registry bucket")
	fs.DurationVar(&backendConversationTTL, "backend-conversation-ttl", 10*time.Minute, "Conversation lifetime (JetStream TTL)")
	fs.DurationVar(&backendResponseTimeout, "backend-response-timeout", 2*time.Minute, "Timeout waiting for replies from NATS")
	fs.StringVar(&backendHashSecret, "backend-hash-secret", "", "Optional secret used for hash_koldun HMAC (base64/plain)")
	fs.StringVar(&backendRootImage, "backend-root-image", "", "Container image for Dllama root pods")
	fs.StringVar(&backendWorkerImage, "backend-worker-image", "", "Container image for Dllama worker pods")

	fs.StringVar(&operatorNATSURL, "operator-nats-url", "", "NATS endpoint used by the operator to reconcile conversations and publish registry data")
	fs.StringVar(&operatorKVBucket, "operator-kv-bucket", "", "JetStream KeyValue bucket containing conversation records")
	fs.StringVar(&operatorTTLPrefix, "operator-ttl-prefix", "nats_ttl_", "Prefix for conversation TTL keys")
	fs.DurationVar(&operatorPollInterval, "operator-poll-interval", 10*time.Second, "Polling interval for operator conversation sync")
	fs.StringVar(&operatorModelsBucket, "operator-models-bucket", "", "JetStream KeyValue bucket where ready models are published (default koldun_models)")
	fs.StringVar(&operatorTokensBucket, "operator-tokens-bucket", "", "JetStream KeyValue bucket where API tokens are published (default koldun_tokens)")
	fs.StringVar(&operatorModelPrefix, "operator-model-prefix", "", "Key prefix for model entries in the registry bucket (default model/)")
	fs.StringVar(&operatorTokenPrefix, "operator-token-prefix", "", "Key prefix for token entries in the registry bucket (default token/)")

	klog.InitFlags(fs)

	args := os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		mode = args[0]
		args = args[1:]
	}

	if err := fs.Parse(args); err != nil {
		logrus.WithError(err).Fatal("parse flags")
	}

	klog.SetOutput(os.Stderr)
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})

	ctx := signals.SetupSignalContext()

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
		)
	case "llm":
		runLLM(ctx, llm.Config{
			Hash:           llmHash,
			ListenAddress:  llmListen,
			NATSURL:        llmNATSURL,
			InPrefix:       llmInPrefix,
			OutPrefix:      llmOutPrefix,
			SidecarURL:     llmSidecarURL,
			SidecarTimeout: llmSidecarTimeout,
			HealthOnly:     llmHealthOnly,
		})
	case "backend", "ingress":
		runIngress(ctx, ingress.Config{
			ListenAddress:      backendListen,
			Namespace:          backendNamespace,
			RootImage:          backendRootImage,
			WorkerImage:        backendWorkerImage,
			NATSURL:            backendNATSURL,
			ConversationBucket: backendConversationBucket,
			ModelsBucket:       backendModelsBucket,
			TokensBucket:       backendTokensBucket,
			InPrefix:           backendInPrefix,
			OutPrefix:          backendOutPrefix,
			TTLPrefix:          backendTTLPrefix,
			ModelPrefix:        backendModelPrefix,
			TokenPrefix:        backendTokenPrefix,
			ConversationTTL:    backendConversationTTL,
			ResponseTimeout:    backendResponseTimeout,
			HashSecret:         []byte(backendHashSecret),
		})
	default:
		logrus.Fatalf("unknown mode %q", mode)
	}
}

func runOperator(ctx context.Context, kubeconfig string, convCfg controllers.ConversationConfig, registryCfg controllers.RegistryConfig) {
	cfg, err := kube.BuildConfig(kubeconfig)
	if err != nil {
		logrus.Fatalf("failed to build Kubernetes config: %v", err)
	}

	manager, err := controllers.NewManager(cfg)
	if err != nil {
		logrus.Fatalf("failed to create controller manager: %v", err)
	}

	if err := manager.Register(ctx); err != nil {
		logrus.Fatalf("failed to register controllers: %v", err)
	}

	if err := controllers.StartRegistrySync(ctx, manager, registryCfg); err != nil {
		logrus.Fatalf("failed to start registry sync: %v", err)
	}

	if err := controllers.StartConversationReconciler(ctx, manager, convCfg); err != nil {
		logrus.Fatalf("failed to start conversation reconciler: %v", err)
	}

	logrus.Info("starting koldun operator")
	klog.Info("koldun operator is starting up")
	if err := manager.Start(ctx); err != nil {
		klog.Errorf("controller manager exited with error: %v", err)
		logrus.Fatalf("controller manager exited with error: %v", err)
	}
}

func runLLM(ctx context.Context, cfg llm.Config) {
	server, err := llm.New(cfg)
	if err != nil {
		logrus.Fatalf("failed to initialise llm server: %v", err)
	}
	if err := server.Run(ctx); err != nil {
		logrus.Fatalf("llm server exited with error: %v", err)
	}
}

func runIngress(ctx context.Context, cfg ingress.Config) {
	server, err := ingress.New(cfg)
	if err != nil {
		logrus.Fatalf("failed to initialise ingress server: %v", err)
	}
	if err := server.Run(ctx); err != nil {
		logrus.Fatalf("ingress server exited with error: %v", err)
	}
}
