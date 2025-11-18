package ingress

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorizond/koldun/pkg/api/openai"
	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/metrics"
	"github.com/gorizond/koldun/pkg/registry"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/sirupsen/logrus"
)

func sanitizeSessionHash(hash string) string {
	h := strings.ToLower(strings.TrimSpace(hash))
	if len(h) > 32 {
		h = h[:32]
	}
	return h
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

func responseSubjectPrefix(outPrefix, hash string) string {
	prefix := ensureTrailingDot(outPrefix)
	return fmt.Sprintf("%s%s.", prefix, hash)
}

func sessionBacklogSubject(hash string) string {
	return fmt.Sprintf("sessions.%s.requests", sanitizeSessionHash(hash))
}

func dllamaSubjectPrefix(hash string) string {
	return fmt.Sprintf("sessions.%s.dllama.", sanitizeSessionHash(hash))
}

func assignmentsBucketName(hash string) string {
	return fmt.Sprintf("sess_%s_assign", sanitizeSessionHash(hash))
}

func stateStreamName(hash string) string {
	return strings.ToUpper(fmt.Sprintf("sess_%s_state", sanitizeSessionHash(hash)))
}

func newRequestID() string {
	return nuid.New().Next()
}

const (
	defaultListenAddress      = ":8082"
	defaultNamespace          = "default"
	defaultConversationBucket = "koldun_ttl"
	defaultModelsBucket       = registry.DefaultModelBucket
	defaultTokensBucket       = registry.DefaultTokenBucket
	defaultInPrefix           = "in."
	defaultOutPrefix          = "out."
	defaultTTLPrefix          = "nats_ttl_"
	defaultModelPrefix        = registry.DefaultModelPrefix
	defaultTokenPrefix        = registry.DefaultTokenPrefix
	tokenCacheTTL             = 5 * time.Minute

	llmRequestStreamName = "KOLDUN_LLM_REQUESTS"

	corsAllowHeaders  = "Authorization, Content-Type, X-Requested-With, X-Api-Key, X-API-Key, X-Auth-Token, KOLDUN_API_TOKEN, OLLMANA_API_KEY"
	corsAllowMethods  = "GET, POST, OPTIONS"
	corsExposeHeaders = "Content-Type"

	errConversationQueueMisconfigured = "conversation queue misconfigured"
)

// Config drives the behaviour of the ingress worker that bridges HTTP chat requests to NATS.
type Config struct {
	ListenAddress string
	Namespace     string
	RootImage     string
	WorkerImage   string

	NATSURL            string
	ConversationBucket string
	ModelsBucket       string
	TokensBucket       string
	InPrefix           string
	OutPrefix          string
	TTLPrefix          string
	ModelPrefix        string
	TokenPrefix        string

	ConversationTTL time.Duration
	ResponseTimeout time.Duration

	SessionMinDllamas              int32
	SessionMaxDllamas              int32
	SessionScaleUpBacklog          int32
	SessionScaleDownIdleSeconds    int32
	SessionDispatcherImage         string
	SessionDispatcherMetricsListen string

	HashSecret     []byte
	AllowAnonymous bool

	ReplicaPower int32

	Logger *logrus.Entry
}

// Server consumes chat completion requests, coordinates TTL records, and relays messages over NATS for ingress traffic.
type Server struct {
	cfg Config
	log *logrus.Entry

	raw      *nats.Conn
	nc       nats.JetStreamContext
	convKV   nats.KeyValue
	modelsKV nats.KeyValue
	tokensKV nats.KeyValue

	httpServer *http.Server
	streamName string
	stateSub   *nats.Subscription

	tokenCache struct {
		mu      sync.RWMutex
		values  map[string]tokenEntry
		expires time.Time
	}

	sessionLoad struct {
		mu           sync.Mutex
		values       map[string]int32
		lastActivity map[string]time.Time
		idleTimers   map[string]*time.Timer
	}

	stateCache struct {
		mu      sync.RWMutex
		workers map[string]map[string]cachedWorkerState
	}

	afterResponseSubscribe func() // test hook invoked after response subscription is created
	ensureConversationHook func(context.Context, string, *registry.Model, int32) (*conversation.Record, error)
}

type tokenEntry struct {
	disabled bool
}

type cachedWorkerState struct {
	state   string
	active  int32
	updated time.Time
}

// New constructs the ingress server.
func New(cfg Config) (*Server, error) {
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = defaultListenAddress
	}
	if cfg.Namespace == "" {
		cfg.Namespace = defaultNamespace
	}
	if strings.TrimSpace(cfg.RootImage) == "" {
		return nil, errors.New("root image is required")
	}
	if strings.TrimSpace(cfg.WorkerImage) == "" {
		return nil, errors.New("worker image is required")
	}
	if cfg.NATSURL == "" {
		cfg.NATSURL = nats.DefaultURL
	}
	if cfg.ConversationBucket == "" {
		cfg.ConversationBucket = defaultConversationBucket
	}
	if cfg.ModelsBucket == "" {
		cfg.ModelsBucket = defaultModelsBucket
	}
	if cfg.TokensBucket == "" {
		cfg.TokensBucket = defaultTokensBucket
	}
	if cfg.InPrefix == "" {
		cfg.InPrefix = defaultInPrefix
	}
	if cfg.OutPrefix == "" {
		cfg.OutPrefix = defaultOutPrefix
	}
	if cfg.TTLPrefix == "" {
		cfg.TTLPrefix = defaultTTLPrefix
	}
	if cfg.ModelPrefix == "" {
		cfg.ModelPrefix = defaultModelPrefix
	}
	if cfg.TokenPrefix == "" {
		cfg.TokenPrefix = defaultTokenPrefix
	}
	if cfg.ConversationTTL == 0 {
		cfg.ConversationTTL = 10 * time.Minute
	}
	if cfg.ResponseTimeout == 0 {
		cfg.ResponseTimeout = 2 * time.Minute
	}
	cfg.SessionDispatcherMetricsListen = strings.TrimSpace(cfg.SessionDispatcherMetricsListen)
	if cfg.SessionMinDllamas <= 0 {
		cfg.SessionMinDllamas = 1
	}
	if cfg.SessionMaxDllamas > 0 && cfg.SessionMaxDllamas < cfg.SessionMinDllamas {
		cfg.SessionMaxDllamas = cfg.SessionMinDllamas
	}
	if strings.TrimSpace(cfg.SessionDispatcherImage) == "" {
		cfg.SessionDispatcherImage = cfg.RootImage
	}
	if cfg.ReplicaPower < 0 {
		cfg.ReplicaPower = 0
	}

	log := cfg.Logger
	if log == nil {
		log = logrus.StandardLogger().WithField("component", "koldun-ingress")
	}

	raw, err := nats.Connect(cfg.NATSURL, nats.Name("koldun-ingress"))
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}

	js, err := raw.JetStream()
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("jetstream context: %w", err)
	}

	streamName, err := ensureRequestStream(js, cfg.InPrefix)
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("request stream: %w", err)
	}

	convKV, err := ensureBucket(js, &nats.KeyValueConfig{
		Bucket:  cfg.ConversationBucket,
		TTL:     cfg.ConversationTTL,
		History: 1,
	})
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("conversation kv bucket: %w", err)
	}

	modelsKV, err := ensureBucket(js, &nats.KeyValueConfig{
		Bucket:  cfg.ModelsBucket,
		History: 1,
	})
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("models kv bucket: %w", err)
	}

	var tokensKV nats.KeyValue
	if !cfg.AllowAnonymous {
		tokensKV, err = ensureBucket(js, &nats.KeyValueConfig{
			Bucket:  cfg.TokensBucket,
			History: 1,
		})
		if err != nil {
			raw.Close()
			return nil, fmt.Errorf("tokens kv bucket: %w", err)
		}
	}

	srv := &Server{
		cfg:        cfg,
		log:        log,
		raw:        raw,
		nc:         js,
		convKV:     convKV,
		modelsKV:   modelsKV,
		tokensKV:   tokensKV,
		streamName: streamName,
	}
	srv.tokenCache.values = make(map[string]tokenEntry)
	srv.sessionLoad.values = make(map[string]int32)
	srv.stateCache.workers = make(map[string]map[string]cachedWorkerState)

	if err := srv.startStateObserver(); err != nil {
		srv.log.WithError(err).Warn("subscribe dllama state events")
	}

	return srv, nil
}

func ensureBucket(js nats.JetStreamContext, cfg *nats.KeyValueConfig) (nats.KeyValue, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket name cannot be empty")
	}
	kv, err := js.KeyValue(cfg.Bucket)
	if err == nats.ErrBucketNotFound {
		return js.CreateKeyValue(cfg)
	}
	if err != nil {
		return nil, err
	}

	status, sErr := kv.Status()
	if sErr != nil {
		return nil, sErr
	}
	if desired := cfg.TTL; desired > 0 {
		current := status.TTL()
		if current <= 0 || absDuration(current-desired) > time.Second {
			if dErr := js.DeleteKeyValue(cfg.Bucket); dErr != nil {
				return nil, fmt.Errorf("delete kv bucket %s: %w", cfg.Bucket, dErr)
			}
			return js.CreateKeyValue(cfg)
		}
	}

	return kv, nil
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func ensureRequestStream(js nats.JetStreamContext, prefix string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "", fmt.Errorf("in-prefix is required for request stream")
	}
	if !strings.HasSuffix(prefix, ".") {
		return "", fmt.Errorf("in-prefix %q must end with '.' to enable durable delivery", prefix)
	}

	required := map[string]struct{}{
		(prefix + ">"):          {},
		(defaultInPrefix + ">"): {},
	}

	info, err := js.StreamInfo(llmRequestStreamName)
	switch {
	case err == nil:
		for _, subj := range info.Config.Subjects {
			required[strings.TrimSpace(subj)] = struct{}{}
		}

		cfg := info.Config
		cfg.Subjects = uniqueSubjects(required)
		if _, err := js.UpdateStream(&cfg); err != nil {
			return "", fmt.Errorf("update stream: %w", err)
		}
	case errors.Is(err, nats.ErrStreamNotFound):
		cfg := &nats.StreamConfig{
			Name:      llmRequestStreamName,
			Subjects:  uniqueSubjects(required),
			Retention: nats.WorkQueuePolicy,
			Storage:   nats.FileStorage,
		}
		if _, err := js.AddStream(cfg); err != nil {
			return "", fmt.Errorf("create stream: %w", err)
		}
	default:
		return "", fmt.Errorf("stream info: %w", err)
	}

	return llmRequestStreamName, nil
}

func uniqueSubjects(values map[string]struct{}) []string {
	list := make([]string, 0, len(values))
	for subj := range values {
		subj = strings.TrimSpace(subj)
		if subj == "" {
			continue
		}
		list = append(list, subj)
	}
	sort.Strings(list)
	return list
}

// Run starts the HTTP server and blocks until shutdown.
func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics.Handler().ServeHTTP(w, r)
	})
	mux.HandleFunc("/v1/models/", s.handleModel) // Single model (must be before /v1/models)
	mux.HandleFunc("/v1/models", s.handleModels)
	mux.HandleFunc("/v1/chat/completions", s.handleChatCompletions)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.applyCORSHeaders(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		start := time.Now()
		s.log.WithFields(logrus.Fields{
			"method": r.Method,
			"path":   r.URL.Path,
			"addr":   r.RemoteAddr,
		}).Info("inbound request")

		// Wrap response writer to capture status code
		wrw := &responseWriterWrapper{ResponseWriter: w, statusCode: http.StatusOK}
		mux.ServeHTTP(wrw, r)

		duration := time.Since(start)

		// Record metrics
		metrics.IngressRequestsTotal.WithLabelValues(r.Method, r.URL.Path, fmt.Sprintf("%d", wrw.statusCode)).Inc()
		metrics.IngressRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration.Seconds())

		s.log.WithFields(logrus.Fields{
			"method": r.Method,
			"path":   r.URL.Path,
			"addr":   r.RemoteAddr,
			"took":   duration,
			"status": wrw.statusCode,
		}).Info("completed request")
	})

	s.httpServer = &http.Server{
		Addr:              s.cfg.ListenAddress,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		s.log.Infof("ingress listening on %s", s.cfg.ListenAddress)
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.log.WithError(err).Warn("HTTP shutdown error")
		}
		if s.stateSub != nil {
			_ = s.stateSub.Drain()
		}
		s.raw.Drain()
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if s.raw == nil || s.raw.Status() != nats.CONNECTED {
		http.Error(w, "nats not connected", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.raw == nil || s.raw.Status() != nats.CONNECTED {
		http.Error(w, "nats not connected", http.StatusServiceUnavailable)
		return
	}
	if s.convKV == nil {
		http.Error(w, "conversation bucket unavailable", http.StatusServiceUnavailable)
		return
	}
	if s.modelsKV == nil {
		http.Error(w, "models bucket unavailable", http.StatusServiceUnavailable)
		return
	}
	if s.tokensKV == nil && !s.cfg.AllowAnonymous {
		http.Error(w, "tokens bucket unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	models, err := s.listModels()
	if err != nil {
		s.log.WithError(err).Error("list models from registry")
		writeError(w, http.StatusInternalServerError, "failed to list models")
		return
	}

	data := make([]map[string]any, 0, len(models))
	for i := range models {
		model := models[i]
		if !registryModelReady(&model) {
			continue
		}
		// OpenAI-compatible response with Koldun extensions
		modelData := map[string]any{
			"id":       fmt.Sprintf("%s/%s", model.Namespace, model.Name),
			"object":   "model",
			"created":  time.Now().Unix(), // OpenAI required field
			"owned_by": "koldun",          // OpenAI required field
		}
		// Koldun extensions (non-standard but useful)
		if model.DisplayName != "" && model.DisplayName != model.Name {
			modelData["name"] = model.DisplayName
		}
		if model.Namespace != "" {
			modelData["namespace"] = model.Namespace
		}
		if model.ConversionSizeBytes > 0 {
			modelData["size_bytes"] = model.ConversionSizeBytes
			modelData["size_human"] = model.ConversionSizeHuman
		}
		data = append(data, modelData)
	}

	sort.Slice(data, func(i, j int) bool {
		aID := data[i]["id"].(string)
		bID := data[j]["id"].(string)
		return aID < bID
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   data,
	})
}

func (s *Server) handleModel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract model ID from path: /v1/models/{model}
	modelID := strings.TrimPrefix(r.URL.Path, "/v1/models/")
	if modelID == "" || modelID == "/" {
		http.Error(w, "model ID required", http.StatusBadRequest)
		return
	}

	model, err := s.resolveModel(modelID)
	if err != nil {
		s.log.WithError(err).WithField("model", modelID).Warn("resolve model")
		writeError(w, http.StatusNotFound, fmt.Sprintf("model %s not found", modelID))
		return
	}

	// OpenAI-compatible response with Koldun extensions
	modelData := map[string]any{
		"id":       fmt.Sprintf("%s/%s", model.Namespace, model.Name),
		"object":   "model",
		"created":  time.Now().Unix(), // OpenAI required field
		"owned_by": "koldun",          // OpenAI required field
	}
	// Koldun extensions (non-standard but useful)
	if model.DisplayName != "" && model.DisplayName != model.Name {
		modelData["name"] = model.DisplayName
	}
	if model.Namespace != "" {
		modelData["namespace"] = model.Namespace
	}
	if model.ConversionSizeBytes > 0 {
		modelData["size_bytes"] = model.ConversionSizeBytes
		modelData["size_human"] = model.ConversionSizeHuman
	}

	writeJSON(w, http.StatusOK, modelData)
}

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	reqCtx := r.Context()
	authCtx, cancel := context.WithTimeout(reqCtx, s.cfg.ResponseTimeout)
	defer cancel()

	var tokenHash string
	if !s.cfg.AllowAnonymous {
		token := extractAPIToken(r)
		if token == "" {
			writeError(w, http.StatusUnauthorized, "missing api token")
			return
		}
		if err := s.validateToken(authCtx, token); err != nil {
			s.log.WithError(err).Warn("token rejected")
			writeError(w, http.StatusUnauthorized, "invalid api token")
			return
		}
		tokenHash = sha256Hex(token)
	}

	payload, err := readAll(r)
	if err != nil {
		s.log.WithError(err).Warn("read request body")
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var req openai.ChatCompletionRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid chat completion payload")
		return
	}
	if strings.TrimSpace(req.Model) == "" {
		writeError(w, http.StatusBadRequest, "model is required")
		return
	}

	hash, err := conversationHashFromHeaders(r, s.cfg.HashSecret)
	if err != nil {
		s.log.WithError(err).Warn("conversation hash from headers")
		writeError(w, http.StatusBadRequest, "failed to derive conversation id")
		return
	}

	chatID := hash
	chatStart := fmt.Sprintf("%d", time.Now().Unix())

	model, err := s.resolveModel(req.Model)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	load := s.incrementSessionLoad(hash)

	record, err := s.ensureConversation(authCtx, hash, model, load)
	if err != nil {
		s.decrementSessionLoad(hash)
		s.log.WithError(err).Error("ensure conversation record")
		writeError(w, http.StatusInternalServerError, "failed to prepare conversation")
		return
	}

	defer func() {
		remaining := s.decrementSessionLoad(hash)
		if _, err := s.ensureConversation(context.Background(), hash, model, remaining); err != nil {
			s.log.WithError(err).Warn("update conversation record after completion")
		}
	}()

	s.refreshConversationTTL(hash)

	queue := record.Queue
	if queue == nil {
		s.log.WithField("hash", hash).Error("missing conversation queue configuration")
		writeError(w, http.StatusBadGateway, errConversationQueueMisconfigured)
		return
	}

	backlogSubject := strings.TrimSpace(queue.BacklogSubject)
	if backlogSubject == "" {
		s.log.WithField("hash", hash).Error("missing backlog subject for conversation queue")
		writeError(w, http.StatusBadGateway, errConversationQueueMisconfigured)
		return
	}

	responsePrefix := strings.TrimSpace(queue.ResponseSubjectPrefix)
	if responsePrefix == "" {
		responsePrefix = responseSubjectPrefix(s.cfg.OutPrefix, hash)
	}
	responseSubject := responsePrefix + newRequestID()

	respCtx := reqCtx
	var respCancel context.CancelFunc
	if req.Stream {
		respCtx, respCancel = context.WithCancel(reqCtx)
	} else {
		respCtx, respCancel = context.WithTimeout(reqCtx, s.cfg.ResponseTimeout)
	}
	defer respCancel()

	dllamaPrefix := strings.TrimSpace(queue.DllamaSubjectPrefix)
	if dllamaPrefix == "" {
		dllamaPrefix = dllamaSubjectPrefix(hash)
	}
	if load == 1 {
		warmCtx, warmCancel := context.WithTimeout(reqCtx, s.cfg.ResponseTimeout)
		err := s.waitForIdleWorker(warmCtx, dllamaPrefix)
		warmCancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			s.log.WithError(err).WithField("hash", hash).Warn("wait for idle worker")
		}
	}

	msgs := make(chan *nats.Msg, 32)
	sub, err := s.raw.ChanSubscribe(responseSubject, msgs)
	if err != nil {
		s.log.WithError(err).Error("subscribe out subject")
		writeError(w, http.StatusInternalServerError, "failed to subscribe to conversation stream")
		return
	}
	defer func() {
		_ = sub.Unsubscribe()
		close(msgs)
	}()

	if hook := s.afterResponseSubscribe; hook != nil {
		hook()
	}

	requestID := newRequestID()

	reqPayload := struct {
		Hash            string                       `json:"hash"`
		ChatID          string                       `json:"chatId"`
		ChatStart       string                       `json:"chatStart"`
		TokenHash       string                       `json:"tokenHash"`
		Model           string                       `json:"model"`
		Namespace       string                       `json:"namespace"`
		Request         openai.ChatCompletionRequest `json:"request"`
		ResponseSubject string                       `json:"responseSubject"`
		RequestID       string                       `json:"requestId"`
	}{
		Hash:            hash,
		ChatID:          chatID,
		ChatStart:       chatStart,
		TokenHash:       tokenHash,
		Model:           record.Model,
		Namespace:       record.Namespace,
		Request:         req,
		ResponseSubject: responseSubject,
		RequestID:       requestID,
	}

	payloadBody, err := json.Marshal(reqPayload)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to marshal request payload")
		return
	}

	backlogMsg := conversation.BacklogMessage{
		ID:        requestID,
		Payload:   payloadBody,
		CreatedAt: time.Now().Unix(),
	}
	body, err := json.Marshal(backlogMsg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to marshal backlog envelope")
		return
	}

	if err := s.raw.Publish(backlogSubject, body); err != nil {
		s.log.WithError(err).Error("publish request")
		writeError(w, http.StatusBadGateway, "failed to enqueue request")
		return
	}

	if req.Stream {
		s.streamResponse(respCtx, w, msgs)
		return
	}

	msg, err := waitForMessage(respCtx, msgs)
	if err != nil {
		writeError(w, http.StatusGatewayTimeout, "timeout waiting for response")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(msg.Data)
}

func (s *Server) ensureConversation(ctx context.Context, hash string, model *registry.Model, active int32) (*conversation.Record, error) {
	if hook := s.ensureConversationHook; hook != nil {
		return hook(ctx, hash, model, active)
	}

	s.log.WithFields(logrus.Fields{
		"hash":     hash,
		"nats_url": s.cfg.NATSURL,
		"model":    model.Name,
	}).Info("ensureConversation called")

	key := s.cfg.TTLPrefix + hash
	rev, err := s.convKV.Get(key)
	if err == nil {
		record, parseErr := conversation.ParseRecord(rev.Value())
		if parseErr == nil {
			recordChanged := false
			requiredReplica := s.replicaPowerForModel(model)
			if record.ReplicaPower != requiredReplica {
				record.ReplicaPower = requiredReplica
				recordChanged = true
			}
			if record.RootImage != s.cfg.RootImage {
				record.RootImage = s.cfg.RootImage
				recordChanged = true
			}
			if record.WorkerImage != s.cfg.WorkerImage {
				record.WorkerImage = s.cfg.WorkerImage
				recordChanged = true
			}
			if record.DispatcherImage != s.cfg.SessionDispatcherImage {
				record.DispatcherImage = s.cfg.SessionDispatcherImage
				recordChanged = true
			}
			if record.DispatcherMetricsListen != s.cfg.SessionDispatcherMetricsListen {
				record.DispatcherMetricsListen = s.cfg.SessionDispatcherMetricsListen
				recordChanged = true
			}
			if record.NATS.URL != s.cfg.NATSURL {
				s.log.WithFields(logrus.Fields{
					"hash":         hash,
					"old_nats_url": record.NATS.URL,
					"new_nats_url": s.cfg.NATSURL,
				}).Info("updating conversation record NATS URL")
				record.NATS.URL = s.cfg.NATSURL
				recordChanged = true
			}
			if strings.TrimSpace(record.Session) == "" {
				record.Session = fmt.Sprintf("session-%s", sanitizeSessionHash(hash))
				recordChanged = true
			}
			if record.Queue == nil {
				record.Queue = &conversation.QueueConfig{}
				recordChanged = true
			}
			backlogSubject := sessionBacklogSubject(hash)
			if record.Queue.BacklogSubject != backlogSubject {
				record.Queue.BacklogSubject = backlogSubject
				recordChanged = true
			}
			responsePrefix := responseSubjectPrefix(s.cfg.OutPrefix, hash)
			if record.Queue.ResponseSubjectPrefix != responsePrefix {
				record.Queue.ResponseSubjectPrefix = responsePrefix
				recordChanged = true
			}
			dllamaPrefix := dllamaSubjectPrefix(hash)
			if record.Queue.DllamaSubjectPrefix != dllamaPrefix {
				record.Queue.DllamaSubjectPrefix = dllamaPrefix
				recordChanged = true
			}
			assignmentsBucket := assignmentsBucketName(hash)
			if record.Queue.AssignmentsBucket != assignmentsBucket {
				record.Queue.AssignmentsBucket = assignmentsBucket
				recordChanged = true
			}
			stateStream := stateStreamName(hash)
			if record.Queue.StateStream != stateStream {
				record.Queue.StateStream = stateStream
				recordChanged = true
			}
			if record.Scaling == nil {
				record.Scaling = &conversation.SessionScalingConfig{}
				recordChanged = true
			}
			if record.Scaling.MinDllamas != s.cfg.SessionMinDllamas {
				record.Scaling.MinDllamas = s.cfg.SessionMinDllamas
				recordChanged = true
			}
			if record.Scaling.MaxDllamas != s.cfg.SessionMaxDllamas {
				record.Scaling.MaxDllamas = s.cfg.SessionMaxDllamas
				recordChanged = true
			}
			if record.Scaling.ScaleUpBacklog != s.cfg.SessionScaleUpBacklog {
				record.Scaling.ScaleUpBacklog = s.cfg.SessionScaleUpBacklog
				recordChanged = true
			}
			if record.Scaling.ScaleDownIdleSeconds != s.cfg.SessionScaleDownIdleSeconds {
				record.Scaling.ScaleDownIdleSeconds = s.cfg.SessionScaleDownIdleSeconds
				recordChanged = true
			}
			desired := active
			if desired < s.cfg.SessionMinDllamas {
				desired = s.cfg.SessionMinDllamas
			}
			if s.cfg.SessionMaxDllamas > 0 && desired > s.cfg.SessionMaxDllamas {
				desired = s.cfg.SessionMaxDllamas
			}
			if record.Scaling.DesiredDllamas != desired {
				record.Scaling.DesiredDllamas = desired
				recordChanged = true
			}
			if record.Scaling.ActiveRequests != active {
				record.Scaling.ActiveRequests = active
				recordChanged = true
			}
			if recordChanged {
				data, err := record.Marshal()
				if err == nil {
					_, err = s.convKV.Update(key, data, rev.Revision())
					if err != nil {
						s.log.WithError(err).Warn("update conversation record")
					}
				}
			}
			return record, nil
		}
		// fallthrough to create new record if parsing failed
	} else if err != nil && err != nats.ErrKeyNotFound {
		return nil, err
	}

	requiredReplica := s.replicaPowerForModel(model)

	modelNamespace := model.Namespace
	if strings.TrimSpace(modelNamespace) == "" {
		modelNamespace = s.cfg.Namespace
	}

	desired := active
	if desired < s.cfg.SessionMinDllamas {
		desired = s.cfg.SessionMinDllamas
	}
	if s.cfg.SessionMaxDllamas > 0 && desired > s.cfg.SessionMaxDllamas {
		desired = s.cfg.SessionMaxDllamas
	}

	record := &conversation.Record{
		Hash:                    hash,
		Session:                 fmt.Sprintf("session-%s", sanitizeSessionHash(hash)),
		Namespace:               s.cfg.Namespace,
		Model:                   fmt.Sprintf("%s/%s", modelNamespace, model.Name),
		CreatedAt:               time.Now().Unix(),
		ReplicaPower:            requiredReplica,
		RootImage:               s.cfg.RootImage,
		WorkerImage:             s.cfg.WorkerImage,
		DispatcherImage:         s.cfg.SessionDispatcherImage,
		DispatcherMetricsListen: s.cfg.SessionDispatcherMetricsListen,
		NATS:                    conversation.NATSConfig{URL: s.cfg.NATSURL},
		Queue: &conversation.QueueConfig{
			BacklogSubject:        sessionBacklogSubject(hash),
			ResponseSubjectPrefix: responseSubjectPrefix(s.cfg.OutPrefix, hash),
			AssignmentsBucket:     assignmentsBucketName(hash),
			DllamaSubjectPrefix:   dllamaSubjectPrefix(hash),
			StateStream:           stateStreamName(hash),
		},
		Scaling: &conversation.SessionScalingConfig{
			MinDllamas:           s.cfg.SessionMinDllamas,
			MaxDllamas:           s.cfg.SessionMaxDllamas,
			ScaleUpBacklog:       s.cfg.SessionScaleUpBacklog,
			ScaleDownIdleSeconds: s.cfg.SessionScaleDownIdleSeconds,
			DesiredDllamas:       desired,
			ActiveRequests:       active,
		},
	}

	s.log.WithFields(logrus.Fields{
		"hash":     hash,
		"nats_url": s.cfg.NATSURL,
		"session":  record.Session,
	}).Info("creating conversation session record with NATS URL")

	data, err := record.Marshal()
	if err != nil {
		return nil, err
	}
	if _, err := s.convKV.Put(key, data); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Server) validateToken(ctx context.Context, plaintext string) error {
	candidate := strings.TrimSpace(plaintext)
	hash := sha256Hex(candidate)
	if s.lookupToken(ctx, hash) || (isHexDigest(candidate) && s.lookupToken(ctx, candidate)) {
		return nil
	}

	s.invalidateTokenCache()
	if s.lookupToken(ctx, hash) || (isHexDigest(candidate) && s.lookupToken(ctx, candidate)) {
		return nil
	}

	return fmt.Errorf("token not found")
}

func (s *Server) lookupToken(ctx context.Context, hash string) bool {
	h := strings.ToLower(strings.TrimSpace(hash))
	cache := s.loadTokenCache(ctx)
	entry, ok := cache[h]
	if !ok {
		return false
	}
	if entry.disabled {
		return false
	}
	return true
}

func (s *Server) invalidateTokenCache() {
	s.tokenCache.mu.Lock()
	s.tokenCache.expires = time.Time{}
	s.tokenCache.mu.Unlock()
}

func (s *Server) incrementSessionLoad(hash string) int32 {
	s.sessionLoad.mu.Lock()
	defer s.sessionLoad.mu.Unlock()
	if s.sessionLoad.values == nil {
		s.sessionLoad.values = make(map[string]int32)
	}
	if s.sessionLoad.lastActivity == nil {
		s.sessionLoad.lastActivity = make(map[string]time.Time)
	}
	if s.sessionLoad.idleTimers == nil {
		s.sessionLoad.idleTimers = make(map[string]*time.Timer)
	}
	if timer := s.sessionLoad.idleTimers[hash]; timer != nil {
		if timer.Stop() {
			delete(s.sessionLoad.idleTimers, hash)
		}
	}

	s.sessionLoad.values[hash]++
	s.sessionLoad.lastActivity[hash] = time.Now()
	return s.sessionLoad.values[hash]
}

func (s *Server) decrementSessionLoad(hash string) int32 {
	s.sessionLoad.mu.Lock()
	defer s.sessionLoad.mu.Unlock()
	if s.sessionLoad.values == nil {
		return 0
	}
	current, ok := s.sessionLoad.values[hash]
	if !ok {
		return 0
	}

	current--
	now := time.Now()
	if current <= 0 {
		delete(s.sessionLoad.values, hash)
		if s.sessionLoad.lastActivity == nil {
			s.sessionLoad.lastActivity = make(map[string]time.Time)
		}
		s.sessionLoad.lastActivity[hash] = now
		if s.cfg.SessionScaleDownIdleSeconds > 0 {
			s.scheduleSessionCleanupLocked(hash, now)
		} else {
			delete(s.sessionLoad.lastActivity, hash)
		}
		return 0
	}

	s.sessionLoad.values[hash] = current
	if s.sessionLoad.lastActivity == nil {
		s.sessionLoad.lastActivity = make(map[string]time.Time)
	}
	s.sessionLoad.lastActivity[hash] = now
	return current
}

func (s *Server) sessionLoadValue(hash string) int32 {
	s.sessionLoad.mu.Lock()
	defer s.sessionLoad.mu.Unlock()
	if s.sessionLoad.values == nil {
		return 0
	}
	return s.sessionLoad.values[hash]
}

func (s *Server) scheduleSessionCleanupLocked(hash string, last time.Time) {
	idleAfter := time.Duration(s.cfg.SessionScaleDownIdleSeconds) * time.Second
	if idleAfter <= 0 {
		return
	}
	if s.sessionLoad.idleTimers == nil {
		s.sessionLoad.idleTimers = make(map[string]*time.Timer)
	}
	if timer := s.sessionLoad.idleTimers[hash]; timer != nil {
		timer.Stop()
	}

	deadline := last.Add(idleAfter)
	timer := time.AfterFunc(idleAfter, func() {
		s.finalizeSessionCleanup(hash, deadline)
	})
	s.sessionLoad.idleTimers[hash] = timer
}

func (s *Server) finalizeSessionCleanup(hash string, deadline time.Time) {
	idleAfter := time.Duration(s.cfg.SessionScaleDownIdleSeconds) * time.Second
	if idleAfter <= 0 {
		return
	}

	s.sessionLoad.mu.Lock()
	var load int32
	if s.sessionLoad.values != nil {
		load = s.sessionLoad.values[hash]
	}
	var last time.Time
	if s.sessionLoad.lastActivity != nil {
		last = s.sessionLoad.lastActivity[hash]
	}
	delete(s.sessionLoad.idleTimers, hash)
	if load > 0 || (!last.IsZero() && last.After(deadline)) {
		s.sessionLoad.mu.Unlock()
		return
	}
	delete(s.sessionLoad.lastActivity, hash)
	s.sessionLoad.mu.Unlock()

	if err := s.deleteConversationRecord(hash); err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			s.log.WithField("hash", hash).Debug("conversation record already deleted during idle cleanup")
			return
		}
		s.log.WithError(err).WithField("hash", hash).Warn("delete idle conversation record")
		return
	}

	s.log.WithField("hash", hash).Info("removed idle conversation record")
}

func (s *Server) deleteConversationRecord(hash string) error {
	if s.convKV == nil {
		return nil
	}
	key := s.cfg.TTLPrefix + hash
	return s.convKV.Delete(key)
}

func (s *Server) loadTokenCache(ctx context.Context) map[string]tokenEntry {
	if s.tokensKV == nil {
		s.tokenCache.mu.Lock()
		if s.tokenCache.values == nil {
			s.tokenCache.values = make(map[string]tokenEntry)
		}
		s.tokenCache.expires = time.Now().Add(tokenCacheTTL)
		s.tokenCache.mu.Unlock()
		return s.tokenCache.values
	}

	s.tokenCache.mu.RLock()
	if time.Now().Before(s.tokenCache.expires) {
		defer s.tokenCache.mu.RUnlock()
		return s.tokenCache.values
	}
	s.tokenCache.mu.RUnlock()

	s.tokenCache.mu.Lock()
	defer s.tokenCache.mu.Unlock()

	if time.Now().Before(s.tokenCache.expires) {
		return s.tokenCache.values
	}

	keys, err := s.tokensKV.Keys()
	if err == nats.ErrNoKeysFound {
		s.tokenCache.values = map[string]tokenEntry{}
		s.tokenCache.expires = time.Now().Add(tokenCacheTTL)
		return s.tokenCache.values
	}
	if err != nil {
		s.log.WithError(err).Warn("list token keys")
		return s.tokenCache.values
	}

	values := make(map[string]tokenEntry, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, s.cfg.TokenPrefix) {
			continue
		}
		rev, err := s.tokensKV.Get(key)
		if err != nil {
			if err != nats.ErrKeyNotFound {
				s.log.WithError(err).WithField("key", key).Warn("get token record")
			}
			continue
		}
		var token registry.Token
		if err := json.Unmarshal(rev.Value(), &token); err != nil {
			s.log.WithError(err).WithField("key", key).Warn("unmarshal token record")
			continue
		}
		hash := strings.ToLower(strings.TrimSpace(token.Hash))
		if hash == "" {
			// fallback to key suffix
			hash = strings.TrimPrefix(key, s.cfg.TokenPrefix)
		}
		if hash == "" {
			continue
		}
		values[hash] = tokenEntry{disabled: token.Disabled}
	}

	s.tokenCache.values = values
	s.tokenCache.expires = time.Now().Add(tokenCacheTTL)
	return values
}

func (s *Server) listModels() ([]registry.Model, error) {
	if s.modelsKV == nil {
		return nil, errors.New("models bucket unavailable")
	}
	keys, err := s.modelsKV.Keys()
	if err == nats.ErrNoKeysFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	models := make([]registry.Model, 0, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, s.cfg.ModelPrefix) {
			continue
		}
		rev, err := s.modelsKV.Get(key)
		if err != nil {
			if err != nats.ErrKeyNotFound {
				s.log.WithError(err).WithField("key", key).Warn("get model record")
			}
			continue
		}
		var model registry.Model
		if err := json.Unmarshal(rev.Value(), &model); err != nil {
			s.log.WithError(err).WithField("key", key).Warn("unmarshal model record")
			continue
		}
		s.populateModelDefaults(&model, key)
		models = append(models, model)
	}
	return models, nil
}

func (s *Server) resolveModel(identifier string) (*registry.Model, error) {
	namespace := s.cfg.Namespace
	name := identifier
	parts := strings.Split(identifier, "/")
	if len(parts) == 2 {
		namespace = strings.TrimSpace(parts[0])
		name = parts[1]
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("model identifier cannot be empty")
	}

	key := s.modelKey(namespace, name)
	rev, err := s.modelsKV.Get(key)
	if err != nil {
		return nil, fmt.Errorf("model %s/%s not found", namespace, name)
	}
	var model registry.Model
	if err := json.Unmarshal(rev.Value(), &model); err != nil {
		return nil, fmt.Errorf("decode model %s/%s: %w", namespace, name, err)
	}
	s.populateModelDefaults(&model, key)
	if !registryModelReady(&model) {
		return nil, fmt.Errorf("model %s/%s is not ready", model.Namespace, model.Name)
	}
	return &model, nil
}

func (s *Server) populateModelDefaults(model *registry.Model, key string) {
	if strings.TrimSpace(model.Namespace) == "" {
		rest := strings.TrimPrefix(key, s.cfg.ModelPrefix)
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) == 2 {
			model.Namespace = parts[0]
			if strings.TrimSpace(model.Name) == "" {
				model.Name = parts[1]
			}
		}
	}
	if strings.TrimSpace(model.Name) == "" {
		rest := strings.TrimPrefix(key, s.cfg.ModelPrefix)
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) == 2 {
			model.Name = parts[1]
		}
	}
}

func (s *Server) replicaPowerForModel(model *registry.Model) int32 {
	if s.cfg.ReplicaPower > 0 {
		return s.cfg.ReplicaPower
	}
	if model != nil && model.ReplicaPower > 0 {
		return model.ReplicaPower
	}
	return 1
}

func (s *Server) modelKey(namespace, name string) string {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = s.cfg.Namespace
	}
	return fmt.Sprintf("%s%s/%s", s.cfg.ModelPrefix, ns, strings.TrimSpace(name))
}

func registryModelReady(model *registry.Model) bool {
	if model == nil {
		return false
	}
	if strings.TrimSpace(model.OutputPVCName) == "" {
		return false
	}
	if model.ConversionSizeBytes <= 0 && strings.TrimSpace(model.ConversionSizeHuman) == "" {
		return false
	}
	return true
}

func isHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (s *Server) refreshConversationTTL(hash string) {
	if s.cfg.ConversationTTL <= 0 || s.convKV == nil {
		return
	}
	key := s.cfg.TTLPrefix + hash
	rev, err := s.convKV.Get(key)
	if err != nil {
		if err != nats.ErrKeyNotFound {
			s.log.WithError(err).WithField("hash", hash).Warn("get conversation record for ttl refresh")
		}
		return
	}
	if _, err := s.convKV.Update(key, rev.Value(), rev.Revision()); err != nil {
		s.log.WithError(err).WithField("hash", hash).Warn("refresh conversation ttl")
	}
}

func (s *Server) streamResponse(ctx context.Context, w http.ResponseWriter, msgs <-chan *nats.Msg) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	writeChunk := func(payload string) {
		payload = strings.TrimSpace(payload)
		if payload == "" {
			_, _ = fmt.Fprint(w, "data: \n\n")
		} else {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
		}
		flusher.Flush()
	}

	errorChunk := func(message string) {
		message = strings.TrimSpace(message)
		if message == "" {
			message = "stream cancelled"
		}
		payload, err := json.Marshal(openai.ErrorResponse{Error: openai.ErrorBody{Message: message}})
		if err != nil {
			escaped := strings.ReplaceAll(message, "\"", "\\\"")
			writeChunk(fmt.Sprintf(`{"error":{"message":"%s"}}`, escaped))
			return
		}
		writeChunk(string(payload))
	}

	state := streamingNormaliserState{}

	for {
		select {
		case msg := <-msgs:
			if msg == nil {
				errorChunk("subscription closed")
				writeChunk("[DONE]")
				return
			}
			line := strings.TrimSpace(string(msg.Data))
			if line == "" {
				continue
			}
			if strings.EqualFold(line, "[DONE]") {
				writeChunk("[DONE]")
				return
			}

			normalised, err := normaliseStreamingChunk(line, &state)
			if err != nil {
				s.log.WithError(err).Debug("normalise chunk")
				writeChunk(line)
				continue
			}
			writeChunk(normalised)
		case <-ctx.Done():
			errorChunk(ctx.Err().Error())
			writeChunk("[DONE]")
			return
		}
	}
}

func waitForMessage(ctx context.Context, msgs <-chan *nats.Msg) (*nats.Msg, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-msgs:
		if msg == nil {
			return nil, errors.New("subscription closed")
		}
		return msg, nil
	}
}

func (s *Server) waitForIdleWorker(ctx context.Context, prefix string) error {
	if s.raw == nil {
		return nil
	}
	prefix = ensureTrailingDot(strings.TrimSpace(prefix))
	if prefix == "" {
		return nil
	}

	if s.hasCachedIdleWorker(prefix) {
		return nil
	}

	subject := fmt.Sprintf("%s*.state", prefix)

	msgs := make(chan *nats.Msg, 8)
	sub, err := s.raw.ChanSubscribe(subject, msgs)
	if err != nil {
		return fmt.Errorf("subscribe state %s: %w", subject, err)
	}
	defer func() {
		_ = sub.Unsubscribe()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-msgs:
			if msg == nil {
				return fmt.Errorf("state subscription closed")
			}
			var event conversation.WorkerStateEvent
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				continue
			}
			worker := strings.TrimSpace(event.Dllama)
			if worker == "" {
				worker = stateWorkerFromSubject(msg.Subject)
			}
			s.cacheWorkerState(prefix, worker, event.State, event.Active, eventTimestamp(event.Timestamp))
			if strings.EqualFold(event.State, "idle") && event.Active <= 0 {
				return nil
			}
		}
	}
}

const globalStateSubject = "sessions.*.dllama.*.state"

func (s *Server) startStateObserver() error {
	if s.raw == nil {
		return nil
	}
	sub, err := s.raw.Subscribe(globalStateSubject, func(msg *nats.Msg) {
		if msg == nil {
			return
		}
		s.consumeStateEvent(msg.Subject, msg.Data)
	})
	if err != nil {
		return fmt.Errorf("subscribe dllama state events: %w", err)
	}
	s.stateSub = sub
	return nil
}

func (s *Server) consumeStateEvent(subject string, data []byte) {
	prefix := statePrefixFromSubject(subject)
	if prefix == "" {
		return
	}
	var event conversation.WorkerStateEvent
	if err := json.Unmarshal(data, &event); err != nil {
		if s.log != nil {
			s.log.WithError(err).WithField("subject", subject).Debug("invalid state event")
		}
		return
	}
	worker := strings.TrimSpace(event.Dllama)
	if worker == "" {
		worker = stateWorkerFromSubject(subject)
	}
	if worker == "" {
		return
	}
	s.cacheWorkerState(prefix, worker, event.State, event.Active, eventTimestamp(event.Timestamp))
}

func (s *Server) cacheWorkerState(prefix, worker, state string, active int32, ts time.Time) {
	prefix = ensureTrailingDot(strings.TrimSpace(prefix))
	worker = strings.TrimSpace(worker)
	if prefix == "" || worker == "" {
		return
	}
	if active < 0 {
		active = 0
	}
	if ts.IsZero() {
		ts = time.Now()
	}

	s.stateCache.mu.Lock()
	defer s.stateCache.mu.Unlock()

	if s.stateCache.workers == nil {
		s.stateCache.workers = make(map[string]map[string]cachedWorkerState)
	}

	workers := s.stateCache.workers[prefix]
	if workers == nil {
		workers = make(map[string]cachedWorkerState)
		s.stateCache.workers[prefix] = workers
	}

	workers[worker] = cachedWorkerState{
		state:   strings.TrimSpace(state),
		active:  active,
		updated: ts,
	}
}

func (s *Server) hasCachedIdleWorker(prefix string) bool {
	prefix = ensureTrailingDot(strings.TrimSpace(prefix))
	if prefix == "" {
		return false
	}

	cutoff := time.Now().Add(-45 * time.Second)

	s.stateCache.mu.Lock()
	defer s.stateCache.mu.Unlock()

	workers := s.stateCache.workers[prefix]
	if len(workers) == 0 {
		return false
	}

	idle := false
	for name, st := range workers {
		if st.updated.Before(cutoff) {
			delete(workers, name)
			continue
		}
		if strings.EqualFold(st.state, "idle") && st.active <= 0 {
			idle = true
		}
	}
	if len(workers) == 0 {
		delete(s.stateCache.workers, prefix)
	}
	return idle
}

func statePrefixFromSubject(subject string) string {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return ""
	}
	parts := strings.Split(subject, ".")
	if len(parts) < 3 {
		return ""
	}
	prefix := strings.Join(parts[:len(parts)-2], ".")
	if prefix == "" {
		return ""
	}
	return ensureTrailingDot(prefix)
}

func stateWorkerFromSubject(subject string) string {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return ""
	}
	parts := strings.Split(subject, ".")
	if len(parts) < 2 {
		return ""
	}
	return strings.TrimSpace(parts[len(parts)-2])
}

func eventTimestamp(ts int64) time.Time {
	if ts <= 0 {
		return time.Now()
	}
	t := time.Unix(ts, 0)
	if t.IsZero() {
		return time.Now()
	}
	return t
}

type streamingNormaliserState struct {
	roleSent bool
	think    thinkRedactor
}

type thinkRedactor struct {
	buffer  string
	inThink bool
}

type streamingChunk struct {
	ID      string                 `json:"id,omitempty"`
	Object  string                 `json:"object,omitempty"`
	Created int64                  `json:"created,omitempty"`
	Model   string                 `json:"model,omitempty"`
	Choices []streamingChunkChoice `json:"choices"`
}

type streamingChunkChoice struct {
	Index        int                 `json:"index"`
	FinishReason *string             `json:"finish_reason"`
	Delta        streamingChunkDelta `json:"delta"`
}

type streamingChunkDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

func (s *streamingNormaliserState) scrubContent(content string) string {
	if s == nil || content == "" {
		return content
	}
	return s.think.filter(content)
}

func (r *thinkRedactor) filter(content string) string {
	if content == "" {
		return ""
	}

	r.buffer += content
	var out strings.Builder

	for {
		lower := strings.ToLower(r.buffer)

		if r.inThink {
			closeIdx := strings.Index(lower, "</think>")
			if closeIdx == -1 {
				if len(r.buffer) > len("</think>")-1 {
					r.buffer = r.buffer[len(r.buffer)-(len("</think>")-1):]
				}
				return out.String()
			}
			r.buffer = r.buffer[closeIdx+len("</think>"):]
			r.inThink = false
			continue
		}

		openIdx := strings.Index(lower, "<think>")
		closeIdx := strings.Index(lower, "</think>")
		if closeIdx != -1 && (openIdx == -1 || closeIdx < openIdx) {
			// Drop stray closing tag when we aren't tracking a think block.
			r.buffer = r.buffer[closeIdx+len("</think>"):]
			continue
		}

		if openIdx == -1 {
			emitLen := len(r.buffer) - longestThinkSuffix(lower)
			if emitLen <= 0 {
				return out.String()
			}
			out.WriteString(r.buffer[:emitLen])
			r.buffer = r.buffer[emitLen:]
			return out.String()
		}

		if openIdx > 0 {
			out.WriteString(r.buffer[:openIdx])
		}
		r.buffer = r.buffer[openIdx+len("<think>"):]
		r.inThink = true
	}
}

func longestThinkSuffix(lower string) int {
	patterns := []string{"<think>", "</think>"}
	max := 0
	length := len(lower)
	for _, pattern := range patterns {
		limit := len(pattern) - 1
		if limit <= 0 {
			continue
		}
		if limit > length {
			limit = length
		}
		for i := 1; i <= limit; i++ {
			if strings.HasSuffix(lower, pattern[:i]) && i > max {
				max = i
			}
		}
	}
	return max
}

func normaliseStreamingChunk(raw string, state *streamingNormaliserState) (string, error) {
	var chunk streamingChunk
	if err := json.Unmarshal([]byte(raw), &chunk); err != nil {
		return raw, nil
	}
	chunk.Object = "chat.completion.chunk"

	for i := range chunk.Choices {
		choice := &chunk.Choices[i]
		choice.Index = i
		if choice.FinishReason != nil && strings.TrimSpace(*choice.FinishReason) == "" {
			choice.FinishReason = nil
		}

		role := strings.TrimSpace(choice.Delta.Role)
		if role != "" {
			if state != nil && state.roleSent {
				choice.Delta.Role = ""
			} else if state != nil {
				state.roleSent = true
			}
		}
		if strings.TrimSpace(choice.Delta.Role) == "" {
			choice.Delta.Role = ""
		}

		content := choice.Delta.Content
		if state != nil {
			content = state.scrubContent(content)
		}
		if strings.TrimSpace(content) == "" {
			choice.Delta.Content = ""
		} else {
			choice.Delta.Content = content
		}

		if choice.Delta.Role == "" && choice.Delta.Content == "" {
			choice.Delta = streamingChunkDelta{}
		}
	}

	payload, err := json.Marshal(chunk)
	if err != nil {
		return raw, err
	}
	return string(payload), nil
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func readAll(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, 20<<20))
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, openai.ErrorResponse{Error: openai.ErrorBody{Message: message}})
}

func (s *Server) applyCORSHeaders(w http.ResponseWriter, r *http.Request) {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		origin = "*"
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	if origin != "*" {
		addVaryHeader(w.Header(), "Origin")
	}
	w.Header().Set("Access-Control-Allow-Methods", corsAllowMethods)
	w.Header().Set("Access-Control-Allow-Headers", corsAllowHeaders)
	w.Header().Set("Access-Control-Expose-Headers", corsExposeHeaders)
	w.Header().Set("Access-Control-Max-Age", "3600")
}

func addVaryHeader(header http.Header, value string) {
	current := header.Get("Vary")
	if current == "" {
		header.Set("Vary", value)
		return
	}
	for _, part := range strings.Split(current, ",") {
		if strings.EqualFold(strings.TrimSpace(part), value) {
			return
		}
	}
	header.Set("Vary", current+", "+value)
}

func extractAPIToken(r *http.Request) string {
	for _, header := range []string{"KOLDUN_API_TOKEN", "OLLMANA_API_KEY", "X-API-KEY", "X-Api-Key", "X-Auth-Token"} {
		if token := strings.TrimSpace(r.Header.Get(header)); token != "" {
			return token
		}
	}

	if auth := strings.TrimSpace(r.Header.Get("Authorization")); auth != "" {
		lower := strings.ToLower(auth)
		switch {
		case strings.HasPrefix(lower, "bearer "):
			return strings.TrimSpace(auth[7:])
		case strings.HasPrefix(lower, "token "):
			return strings.TrimSpace(auth[6:])
		default:
			if !strings.Contains(auth, " ") {
				return auth
			}
		}
	}

	for _, key := range []string{"api_key", "api-key"} {
		if token := strings.TrimSpace(r.URL.Query().Get(key)); token != "" {
			return token
		}
	}

	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func conversationHashFromHeaders(r *http.Request, secret []byte) (string, error) {
	pairs := make([]string, 0, len(r.Header))
	for name, values := range r.Header {
		lower := strings.ToLower(name)
		if !strings.HasPrefix(lower, "x-") {
			continue
		}
		if lower == "x-forwarded-server" {
			continue
		}
		for _, v := range values {
			pairs = append(pairs, lower+"="+strings.TrimSpace(v))
		}
	}
	// Добавляем User-Agent в хеш
	userAgent := strings.TrimSpace(r.Header.Get("User-Agent"))
	if userAgent != "" {
		pairs = append(pairs, "user-agent="+userAgent)
	}
	if len(pairs) == 0 {
		return "", fmt.Errorf("no identifying headers provided")
	}
	sort.Strings(pairs)
	message := []byte(strings.Join(pairs, "&"))
	if len(secret) == 0 {
		sum := sha256.Sum256(message)
		return hex.EncodeToString(sum[:]), nil
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(message)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func minVal(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// responseWriterWrapper wraps http.ResponseWriter to capture the status code
type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriterWrapper) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}
