package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/controllers"
	"github.com/gorizond/koldun/pkg/servers/dispatcher"
	"github.com/gorizond/koldun/pkg/servers/ingress"
	llmserver "github.com/gorizond/koldun/pkg/servers/llm"
	operatorhealth "github.com/gorizond/koldun/pkg/servers/operator"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func TestTrimBase64Prefix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    string
		expected string
		forced   bool
	}{
		{name: "base64 prefix", value: "base64:Zm9v", expected: "Zm9v", forced: true},
		{name: "b64 prefix", value: "b64:YmFy", expected: "YmFy", forced: true},
		{name: "no prefix", value: "plain", expected: "", forced: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			result, forced := trimBase64Prefix(tt.value)
			require.Equal(t, tt.expected, result)
			require.Equal(t, tt.forced, forced)
		})
	}
}

func TestTryDecodeBase64(t *testing.T) {
	t.Parallel()
	valid := base64.StdEncoding.EncodeToString([]byte("payload"))
	decoded, ok := tryDecodeBase64(valid)
	require.True(t, ok)
	require.Equal(t, []byte("payload"), decoded)

	raw := base64.RawURLEncoding.EncodeToString([]byte("data"))
	decoded, ok = tryDecodeBase64(raw)
	require.True(t, ok)
	require.Equal(t, []byte("data"), decoded)

	invalid, ok := tryDecodeBase64("not-base64!")
	require.False(t, ok)
	require.Nil(t, invalid)
}

func TestDecodeHashSecret(t *testing.T) {
	t.Parallel()
	decoded := decodeHashSecret("")
	require.Nil(t, decoded)

	decoded = decodeHashSecret(" base64:Zm9v ")
	require.Equal(t, []byte("foo"), decoded)

	decoded = decodeHashSecret("b64:Zm9vYmFy")
	require.Equal(t, []byte("foobar"), decoded)

	decoded = decodeHashSecret("base64:%%%")
	require.Equal(t, []byte("%%%"), decoded, "forced prefix should fall back to raw bytes on decode failure")

	decoded = decodeHashSecret("Zm9vLw==")
	require.Equal(t, []byte("foo/"), decoded, "plain base64 without prefix should decode")

	decoded = decodeHashSecret("hash-with-dash")
	require.Equal(t, []byte("hash-with-dash"), decoded, "non-base64 characters without forced prefix should return trimmed value")
}

func TestRunOperatorSuccess(t *testing.T) {
	origBuildConfig := buildConfigFn
	origNewManager := newOperatorManagerFn
	origNewHealth := newHealthServerFn
	origRegistry := startRegistrySyncFn
	origConversation := startConversationReconcilerFn

	t.Cleanup(func() {
		buildConfigFn = origBuildConfig
		newOperatorManagerFn = origNewManager
		newHealthServerFn = origNewHealth
		startRegistrySyncFn = origRegistry
		startConversationReconcilerFn = origConversation
	})

	fakeMgr := newFakeOperatorManager()
	fakeMgr.health.SetAPIHealthy(true)
	fakeMgr.health.SetCachesSynced(true)

	var gotKubeconfig string
	buildConfigFn = func(kubeconfig string) (*rest.Config, error) {
		gotKubeconfig = kubeconfig
		return &rest.Config{}, nil
	}

	registryCalled := make(chan controllers.RegistryConfig, 1)
	conversationCalled := make(chan controllers.ConversationConfig, 1)

	newOperatorManagerFn = func(cfg *rest.Config) (operatorManager, error) {
		return fakeMgr, nil
	}

	healthSrv := &fakeHealthServer{}
	newHealthServerFn = func(cfg operatorhealth.Config) (operatorHealthServer, error) {
		require.Equal(t, ":9090", cfg.ListenAddress)
		require.Equal(t, fakeMgr.health, cfg.Health)
		return healthSrv, nil
	}

	startRegistrySyncFn = func(ctx context.Context, mgr operatorManager, cfg controllers.RegistryConfig) error {
		select {
		case registryCalled <- cfg:
		default:
		}
		return nil
	}

	startConversationReconcilerFn = func(ctx context.Context, mgr operatorManager, cfg controllers.ConversationConfig) error {
		select {
		case conversationCalled <- cfg:
		default:
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	convCfg := controllers.ConversationConfig{
		NATSURL:      "nats://conv",
		KVBucket:     "bucket",
		TTLPrefix:    "ttl_",
		PollInterval: time.Minute,
	}
	regCfg := controllers.RegistryConfig{
		NATSURL:      "nats://registry",
		ModelsBucket: "models",
		TokensBucket: "tokens",
		ModelPrefix:  "model/",
		TokenPrefix:  "token/",
	}

	done := make(chan struct{})
	go func() {
		runOperator(ctx, "/tmp/kubeconfig", convCfg, regCfg, ":9090", true)
		close(done)
	}()

	require.Eventually(t, func() bool {
		return fakeMgr.startCalled()
	}, time.Second, 10*time.Millisecond, "manager Start should be invoked")

	select {
	case cfg := <-registryCalled:
		require.Equal(t, regCfg, cfg)
	case <-time.After(time.Second):
		t.Fatal("startRegistrySyncFn was not invoked")
	}

	select {
	case cfg := <-conversationCalled:
		require.Equal(t, convCfg, cfg)
	case <-time.After(time.Second):
		t.Fatal("startConversationReconcilerFn was not invoked")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runOperator did not return after context cancellation")
	}

	require.Equal(t, "/tmp/kubeconfig", gotKubeconfig)

	require.True(t, fakeMgr.ensureCalled())
	require.False(t, fakeMgr.ensureValue(), "disableBucketEnsure=true should pass false to SetEnsureObjectStorageBuckets")
	require.True(t, fakeMgr.registerCalled())
	require.Eventually(t, func() bool { return !fakeMgr.health.APIHealthy() }, time.Second, 10*time.Millisecond, "health should be marked unhealthy on shutdown")
	require.Eventually(t, func() bool { return !fakeMgr.health.CachesSynced() }, time.Second, 10*time.Millisecond, "cache sync state should be cleared on shutdown")

	require.Equal(t, 1, healthSrv.runCount())
	require.NotNil(t, healthSrv.lastCtx())
}

type fakeOperatorManager struct {
	mu           sync.Mutex
	ensureSet    bool
	ensure       bool
	registered   bool
	startInvoked bool
	health       *controllers.Health
}

func newFakeOperatorManager() *fakeOperatorManager {
	return &fakeOperatorManager{
		health: controllers.NewHealth(),
	}
}

func (f *fakeOperatorManager) SetEnsureObjectStorageBuckets(v bool) {
	f.mu.Lock()
	f.ensureSet = true
	f.ensure = v
	f.mu.Unlock()
}

func (f *fakeOperatorManager) Register(context.Context) error {
	f.mu.Lock()
	f.registered = true
	f.mu.Unlock()
	return nil
}

func (f *fakeOperatorManager) Start(ctx context.Context) error {
	f.mu.Lock()
	f.startInvoked = true
	f.mu.Unlock()
	<-ctx.Done()
	return nil
}

func (f *fakeOperatorManager) Health() *controllers.Health {
	return f.health
}

func (f *fakeOperatorManager) ensureCalled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ensureSet
}

func (f *fakeOperatorManager) ensureValue() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ensure
}

func (f *fakeOperatorManager) registerCalled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registered
}

func (f *fakeOperatorManager) startCalled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.startInvoked
}

type fakeHealthServer struct {
	mu       sync.Mutex
	ctx      context.Context
	runCalls int
}

func (f *fakeHealthServer) Run(ctx context.Context) error {
	f.mu.Lock()
	f.ctx = ctx
	f.runCalls++
	f.mu.Unlock()
	<-ctx.Done()
	return nil
}

func (f *fakeHealthServer) runCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runCalls
}

func (f *fakeHealthServer) lastCtx() context.Context {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ctx
}

func TestMainLLMMode(t *testing.T) {
	origArgs := os.Args
	os.Args = []string{
		"koldun",
		"llm",
		"--llm-hash=test-hash",
		"--llm-sidecar-url=http://127.0.0.1:9000",
	}
	t.Cleanup(func() { os.Args = origArgs })
	stubSignalContext(t)

	origLLM := newLLMServerFn
	stub := &stubRunServer{}
	var captured llmserver.Config
	newLLMServerFn = func(cfg llmserver.Config) (llmServer, error) {
		captured = cfg
		return stub, nil
	}
	t.Cleanup(func() { newLLMServerFn = origLLM })

	origExit := logrus.StandardLogger().ExitFunc
	logrus.StandardLogger().ExitFunc = func(int) {}
	t.Cleanup(func() { logrus.StandardLogger().ExitFunc = origExit })

	main()

	require.Equal(t, 1, stub.runCount())
	require.Equal(t, "test-hash", captured.Hash)
	require.Equal(t, "http://127.0.0.1:9000", captured.SidecarURL)
}

func TestMainDispatcherMode(t *testing.T) {
	origArgs := os.Args
	os.Args = []string{
		"koldun",
		"dispatcher",
		"--dispatcher-hash=test-hash",
		"--dispatcher-nats-url=nats://example:4222",
		"--dispatcher-backlog-subject=sessions.test.requests",
		"--dispatcher-assignments-bucket=koldun_assignments",
		"--dispatcher-dllama-prefix=sessions.test.dllama.",
		"--dispatcher-state-prefix=sessions.test.state.",
		"--dispatcher-queue-group=dispatcher-test",
		"--dispatcher-ack-wait=45s",
		"--dispatcher-metrics-listen=:9099",
	}
	t.Cleanup(func() { os.Args = origArgs })
	stubSignalContext(t)

	origDispatcher := newDispatcherServerFn
	stub := &stubRunServer{}
	var captured dispatcher.Config
	newDispatcherServerFn = func(cfg dispatcher.Config) (dispatcherServer, error) {
		captured = cfg
		return stub, nil
	}
	t.Cleanup(func() { newDispatcherServerFn = origDispatcher })

	origExit := logrus.StandardLogger().ExitFunc
	logrus.StandardLogger().ExitFunc = func(int) {}
	t.Cleanup(func() { logrus.StandardLogger().ExitFunc = origExit })

	main()

	require.Equal(t, 1, stub.runCount())
	require.Equal(t, "test-hash", captured.Hash)
	require.Equal(t, "sessions.test.requests", captured.BacklogSubject)
	require.Equal(t, "koldun_assignments", captured.AssignmentsBucket)
	require.Equal(t, "sessions.test.dllama.", captured.DllamaSubjectPrefix)
	require.Equal(t, "sessions.test.state.", captured.StateSubjectPrefix)
	require.Equal(t, ":9099", captured.MetricsAddr)
	require.Equal(t, 45*time.Second, captured.AckWait)
	require.Equal(t, "dispatcher-test", captured.QueueGroup)
}

func TestRunIngressDelegatesToServer(t *testing.T) {
	origFn := newIngressServerFn
	defer func() { newIngressServerFn = origFn }()

	stub := &stubRunServer{}
	var captured ingress.Config
	newIngressServerFn = func(cfg ingress.Config) (ingressServer, error) {
		captured = cfg
		return stub, nil
	}

	ctx := context.Background()
	cfg := ingress.Config{
		Namespace:          "ns",
		ConversationBucket: "convos",
	}
	runIngress(ctx, cfg)

	require.Equal(t, 1, stub.runCount())
	require.Equal(t, cfg, captured)
}

func TestRunDispatcherDelegatesToServer(t *testing.T) {
	origFn := newDispatcherServerFn
	defer func() { newDispatcherServerFn = origFn }()

	stub := &stubRunServer{}
	var captured dispatcher.Config
	newDispatcherServerFn = func(cfg dispatcher.Config) (dispatcherServer, error) {
		captured = cfg
		return stub, nil
	}

	ctx := context.Background()
	cfg := dispatcher.Config{
		Hash:                "hash",
		NATSURL:             "nats://example:4222",
		BacklogSubject:      "backlog",
		AssignmentsBucket:   "assignments",
		DllamaSubjectPrefix: "dllama.",
	}
	runDispatcher(ctx, cfg)

	require.Equal(t, 1, stub.runCount())
	require.Equal(t, cfg, captured)
}

func TestRunLLMFatalOnInitError(t *testing.T) {
	origFn := newLLMServerFn
	defer func() { newLLMServerFn = origFn }()

	newLLMServerFn = func(cfg llmserver.Config) (llmServer, error) {
		return nil, errors.New("boom")
	}

	expectFatal(t, func() {
		runLLM(context.Background(), llmserver.Config{})
	})
}

func TestRunLLMFatalOnRunError(t *testing.T) {
	origFn := newLLMServerFn
	defer func() { newLLMServerFn = origFn }()

	stub := &stubRunServer{}
	stub.setError(errors.New("run failed"))
	newLLMServerFn = func(cfg llmserver.Config) (llmServer, error) {
		return stub, nil
	}

	expectFatal(t, func() {
		runLLM(context.Background(), llmserver.Config{})
	})
}

func TestRunIngressFatalOnInitError(t *testing.T) {
	origFn := newIngressServerFn
	defer func() { newIngressServerFn = origFn }()

	newIngressServerFn = func(cfg ingress.Config) (ingressServer, error) {
		return nil, errors.New("boom")
	}

	expectFatal(t, func() {
		runIngress(context.Background(), ingress.Config{})
	})
}

func TestRunIngressFatalOnRunError(t *testing.T) {
	origFn := newIngressServerFn
	defer func() { newIngressServerFn = origFn }()

	stub := &stubRunServer{}
	stub.setError(errors.New("ingress failed"))
	newIngressServerFn = func(cfg ingress.Config) (ingressServer, error) {
		return stub, nil
	}

	expectFatal(t, func() {
		runIngress(context.Background(), ingress.Config{})
	})
}

func TestRunDispatcherFatalOnInitError(t *testing.T) {
	origFn := newDispatcherServerFn
	defer func() { newDispatcherServerFn = origFn }()

	newDispatcherServerFn = func(cfg dispatcher.Config) (dispatcherServer, error) {
		return nil, errors.New("boom")
	}

	expectFatal(t, func() {
		runDispatcher(context.Background(), dispatcher.Config{})
	})
}

func TestRunDispatcherFatalOnRunError(t *testing.T) {
	origFn := newDispatcherServerFn
	defer func() { newDispatcherServerFn = origFn }()

	stub := &stubRunServer{}
	stub.setError(errors.New("dispatcher failed"))
	newDispatcherServerFn = func(cfg dispatcher.Config) (dispatcherServer, error) {
		return stub, nil
	}

	expectFatal(t, func() {
		runDispatcher(context.Background(), dispatcher.Config{})
	})
}

func TestMainDispatcherFailsOnQueueMisconfiguration(t *testing.T) {
	logger := logrus.StandardLogger()
	var buf bytes.Buffer
	origOut := logger.Out
	logger.SetOutput(&buf)
	t.Cleanup(func() {
		logger.SetOutput(origOut)
	})

	origArgs := os.Args
	t.Cleanup(func() {
		os.Args = origArgs
	})
	os.Args = []string{
		"koldun",
		"--mode=dispatcher",
		"--dispatcher-hash=test-hash",
		"--dispatcher-nats-url=nats://example:4222",
		"--dispatcher-assignments-bucket=assignments",
		"--dispatcher-dllama-prefix=dllama.",
		"--dispatcher-state-prefix=state.",
		"--dispatcher-backlog-subject=   ",
	}

	stubSignalContext(t)
	expectFatal(t, func() {
		main()
	})

	require.Contains(t, buf.String(), dispatcher.ErrQueueMisconfigured.Error())
}

func TestMainDispatcherFailsOnMissingAssignmentsBucket(t *testing.T) {
	logger := logrus.StandardLogger()
	var buf bytes.Buffer
	origOut := logger.Out
	logger.SetOutput(&buf)
	t.Cleanup(func() {
		logger.SetOutput(origOut)
	})

	origArgs := os.Args
	t.Cleanup(func() {
		os.Args = origArgs
	})
	os.Args = []string{
		"koldun",
		"--mode=dispatcher",
		"--dispatcher-hash=test-hash",
		"--dispatcher-nats-url=nats://example:4222",
		"--dispatcher-assignments-bucket=   ",
		"--dispatcher-dllama-prefix=dllama.",
		"--dispatcher-state-prefix=state.",
		"--dispatcher-backlog-subject=dispatcher.backlog",
	}

	stubSignalContext(t)
	expectFatal(t, func() {
		main()
	})

	require.Contains(t, buf.String(), dispatcher.ErrQueueMisconfigured.Error())
}

func TestMainDispatcherFailsOnMissingDllamaPrefix(t *testing.T) {
	logger := logrus.StandardLogger()
	var buf bytes.Buffer
	origOut := logger.Out
	logger.SetOutput(&buf)
	t.Cleanup(func() {
		logger.SetOutput(origOut)
	})

	origArgs := os.Args
	t.Cleanup(func() {
		os.Args = origArgs
	})
	os.Args = []string{
		"koldun",
		"--mode=dispatcher",
		"--dispatcher-hash=test-hash",
		"--dispatcher-nats-url=nats://example:4222",
		"--dispatcher-assignments-bucket=assignments",
		"--dispatcher-backlog-subject=dispatcher.backlog",
		"--dispatcher-dllama-prefix=   ",
		"--dispatcher-state-prefix=state.",
	}

	stubSignalContext(t)
	expectFatal(t, func() {
		main()
	})

	require.Contains(t, buf.String(), "dllama subject prefix is required")
}

func TestMainDispatcherFailsOnMissingStatePrefix(t *testing.T) {
	logger := logrus.StandardLogger()
	var buf bytes.Buffer
	origOut := logger.Out
	logger.SetOutput(&buf)
	t.Cleanup(func() {
		logger.SetOutput(origOut)
	})

	origArgs := os.Args
	t.Cleanup(func() {
		os.Args = origArgs
	})
	os.Args = []string{
		"koldun",
		"--mode=dispatcher",
		"--dispatcher-hash=test-hash",
		"--dispatcher-nats-url=nats://example:4222",
		"--dispatcher-assignments-bucket=assignments",
		"--dispatcher-backlog-subject=dispatcher.backlog",
		"--dispatcher-dllama-prefix=dllama.",
		"--dispatcher-state-prefix=   ",
	}

	stubSignalContext(t)
	expectFatal(t, func() {
		main()
	})

	require.Contains(t, buf.String(), "dispatcher state subject prefix is required")
}

func TestMainDispatcherFailsOnStatePrefixWithoutDot(t *testing.T) {
	logger := logrus.StandardLogger()
	var buf bytes.Buffer
	origOut := logger.Out
	logger.SetOutput(&buf)
	t.Cleanup(func() {
		logger.SetOutput(origOut)
	})

	origArgs := os.Args
	t.Cleanup(func() {
		os.Args = origArgs
	})
	os.Args = []string{
		"koldun",
		"--mode=dispatcher",
		"--dispatcher-hash=test-hash",
		"--dispatcher-nats-url=nats://example:4222",
		"--dispatcher-assignments-bucket=assignments",
		"--dispatcher-backlog-subject=dispatcher.backlog",
		"--dispatcher-dllama-prefix=dllama.",
		"--dispatcher-state-prefix=dllama",
	}

	stubSignalContext(t)
	expectFatal(t, func() {
		main()
	})

	require.Contains(t, buf.String(), "dispatcher state subject prefix must end with '.'")
}

type stubRunServer struct {
	mu      sync.Mutex
	count   int
	context []context.Context
	err     error
}

func (s *stubRunServer) Run(ctx context.Context) error {
	s.mu.Lock()
	s.count++
	s.context = append(s.context, ctx)
	err := s.err
	s.mu.Unlock()
	return err
}

func (s *stubRunServer) runCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (s *stubRunServer) setError(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func stubSignalContext(t *testing.T) {
	t.Helper()
	orig := setupSignalContextFn
	setupSignalContextFn = func() context.Context {
		return context.Background()
	}
	t.Cleanup(func() {
		setupSignalContextFn = orig
	})
}

type fatalExit struct{}

func expectFatal(t *testing.T, fn func()) {
	t.Helper()

	origExit := logrus.StandardLogger().ExitFunc
	defer func() { logrus.StandardLogger().ExitFunc = origExit }()

	fatalCalled := false
	logrus.StandardLogger().ExitFunc = func(code int) {
		fatalCalled = true
		panic(fatalExit{})
	}

	require.PanicsWithValue(t, fatalExit{}, fn)
	require.True(t, fatalCalled, "expected fatal exit to be triggered")
}
