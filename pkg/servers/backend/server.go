package backend

import (
	"context"
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
	"github.com/gorizond/koldun/pkg/registry"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

const (
	defaultListenAddress      = ":8082"
	defaultNamespace          = "default"
	defaultConversationBucket = "koldun_ttl"
	defaultModelsBucket       = registry.DefaultModelBucket
	defaultTokensBucket       = registry.DefaultTokenBucket
	defaultInPrefix           = "in_"
	defaultOutPrefix          = "out_"
	defaultTTLPrefix          = "nats_ttl_"
	defaultModelPrefix        = registry.DefaultModelPrefix
	defaultTokenPrefix        = registry.DefaultTokenPrefix
	tokenCacheTTL             = 5 * time.Minute
)

// Config drives the behaviour of the backend worker that bridges HTTP chat requests to NATS.
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

	HashSecret []byte

	Logger *logrus.Entry
}

// Server consumes chat completion requests, coordinates TTL records, and relays messages over NATS.
type Server struct {
	cfg Config
	log *logrus.Entry

	raw      *nats.Conn
	nc       nats.JetStreamContext
	convKV   nats.KeyValue
	modelsKV nats.KeyValue
	tokensKV nats.KeyValue

	httpServer *http.Server

	tokenCache struct {
		mu      sync.RWMutex
		values  map[string]tokenEntry
		expires time.Time
	}
}

type tokenEntry struct {
	disabled bool
}

// New constructs the backend server.
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

	log := cfg.Logger
	if log == nil {
		log = logrus.StandardLogger().WithField("component", "koldun-backend")
	}

	raw, err := nats.Connect(cfg.NATSURL, nats.Name("koldun-backend"))
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}

	js, err := raw.JetStream()
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("jetstream context: %w", err)
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

	tokensKV, err := ensureBucket(js, &nats.KeyValueConfig{
		Bucket:  cfg.TokensBucket,
		History: 1,
	})
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("tokens kv bucket: %w", err)
	}

	srv := &Server{
		cfg:      cfg,
		log:      log,
		raw:      raw,
		nc:       js,
		convKV:   convKV,
		modelsKV: modelsKV,
		tokensKV: tokensKV,
	}
	srv.tokenCache.values = make(map[string]tokenEntry)
	return srv, nil
}

func ensureBucket(js nats.JetStreamContext, cfg *nats.KeyValueConfig) (nats.KeyValue, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket name cannot be empty")
	}
	kv, err := js.KeyValue(cfg.Bucket)
	if err == nats.ErrBucketNotFound {
		kv, err = js.CreateKeyValue(cfg)
	}
	return kv, err
}

// Run starts the HTTP server and blocks until shutdown.
func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/v1/models", s.handleModels)
	mux.HandleFunc("/v1/chat/completions", s.handleChatCompletions)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		s.log.WithFields(logrus.Fields{
			"method": r.Method,
			"path":   r.URL.Path,
			"addr":   r.RemoteAddr,
		}).Info("inbound request")
		mux.ServeHTTP(w, r)
		s.log.WithFields(logrus.Fields{
			"method": r.Method,
			"path":   r.URL.Path,
			"addr":   r.RemoteAddr,
			"took":   time.Since(start),
		}).Info("completed request")
	})

	s.httpServer = &http.Server{
		Addr:              s.cfg.ListenAddress,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		s.log.Infof("backend listening on %s", s.cfg.ListenAddress)
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
	if s.tokensKV == nil {
		http.Error(w, "tokens bucket unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
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
		data = append(data, map[string]any{
			"id":         fmt.Sprintf("%s/%s", model.Namespace, model.Name),
			"object":     "model",
			"name":       firstNonEmpty(model.DisplayName, model.Name),
			"namespace":  model.Namespace,
			"size_bytes": model.ConversionSizeBytes,
			"size_human": model.ConversionSizeHuman,
		})
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

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := strings.TrimSpace(r.Header.Get("KOLDUN_API_TOKEN"))
	if token == "" {
		writeError(w, http.StatusUnauthorized, "missing api token")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ResponseTimeout)
	defer cancel()

	if err := s.validateToken(ctx, token); err != nil {
		s.log.WithError(err).Warn("token rejected")
		writeError(w, http.StatusUnauthorized, "invalid api token")
		return
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

	hash, err := conversationHashFromHeaders(r)
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

	record, err := s.ensureConversation(ctx, hash, model)
	if err != nil {
		s.log.WithError(err).Error("ensure conversation record")
		writeError(w, http.StatusInternalServerError, "failed to prepare conversation")
		return
	}

	s.refreshConversationTTL(hash)

	subjectIn := s.cfg.InPrefix + hash
	subjectOut := s.cfg.OutPrefix + hash

	msgs := make(chan *nats.Msg, 32)
	sub, err := s.raw.ChanSubscribe(subjectOut, msgs)
	if err != nil {
		s.log.WithError(err).Error("subscribe out subject")
		writeError(w, http.StatusInternalServerError, "failed to subscribe to conversation stream")
		return
	}
	defer func() {
		_ = sub.Unsubscribe()
		close(msgs)
	}()

	reqPayload := struct {
		Hash      string                       `json:"hash"`
		ChatID    string                       `json:"chatId"`
		ChatStart string                       `json:"chatStart"`
		TokenHash string                       `json:"tokenHash"`
		Model     string                       `json:"model"`
		Namespace string                       `json:"namespace"`
		Request   openai.ChatCompletionRequest `json:"request"`
	}{
		Hash:      hash,
		ChatID:    chatID,
		ChatStart: chatStart,
		TokenHash: sha256Hex(token),
		Model:     record.Model,
		Namespace: record.Namespace,
		Request:   req,
	}

	body, err := json.Marshal(reqPayload)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to marshal request")
		return
	}

	if err := s.raw.Publish(subjectIn, body); err != nil {
		s.log.WithError(err).Error("publish request")
		writeError(w, http.StatusBadGateway, "failed to enqueue request")
		return
	}

	if req.Stream {
		s.streamResponse(ctx, w, msgs)
		return
	}

	msg, err := waitForMessage(ctx, msgs)
	if err != nil {
		writeError(w, http.StatusGatewayTimeout, "timeout waiting for response")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(msg.Data)
}

func (s *Server) ensureConversation(ctx context.Context, hash string, model *registry.Model) (*conversation.Record, error) {
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
			requiredReplica := model.ReplicaPower
			if requiredReplica <= 0 {
				requiredReplica = 1
			}
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
			if record.NATS.URL != s.cfg.NATSURL {
				s.log.WithFields(logrus.Fields{
					"hash":         hash,
					"old_nats_url": record.NATS.URL,
					"new_nats_url": s.cfg.NATSURL,
				}).Info("updating conversation record NATS URL")
				record.NATS.URL = s.cfg.NATSURL
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

	requiredReplica := model.ReplicaPower
	if requiredReplica <= 0 {
		requiredReplica = 1
	}

	modelNamespace := model.Namespace
	if strings.TrimSpace(modelNamespace) == "" {
		modelNamespace = s.cfg.Namespace
	}

	record := &conversation.Record{
		Hash:         hash,
		Dllama:       fmt.Sprintf("dllama-%d-%s", time.Now().Unix(), hash[:minVal(len(hash), 8)]),
		Namespace:    s.cfg.Namespace,
		Model:        fmt.Sprintf("%s/%s", modelNamespace, model.Name),
		CreatedAt:    time.Now().Unix(),
		ReplicaPower: requiredReplica,
		RootImage:    s.cfg.RootImage,
		WorkerImage:  s.cfg.WorkerImage,
		NATS:         conversation.NATSConfig{URL: s.cfg.NATSURL},
	}

	s.log.WithFields(logrus.Fields{
		"hash":     hash,
		"nats_url": s.cfg.NATSURL,
		"dllama":   record.Dllama,
	}).Info("creating conversation record with NATS URL")

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

func (s *Server) loadTokenCache(ctx context.Context) map[string]tokenEntry {
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

	for {
		msg, err := waitForMessage(ctx, msgs)
		if err != nil {
			writeSSE(w, "error", err.Error())
			flusher.Flush()
			return
		}
		line := strings.TrimSpace(string(msg.Data))
		if strings.EqualFold(line, "[DONE]") {
			writeSSE(w, "done", "")
			flusher.Flush()
			return
		}
		writeSSE(w, "message", line)
		flusher.Flush()
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

func writeSSE(w http.ResponseWriter, event, data string) {
	if event != "" {
		_, _ = fmt.Fprintf(w, "event: %s\n", event)
	}
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
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

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func conversationHashFromHeaders(r *http.Request) (string, error) {
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
	sum := sha256.Sum256([]byte(strings.Join(pairs, "&")))
	return hex.EncodeToString(sum[:]), nil
}

func minVal(a, b int) int {
	if a < b {
		return a
	}
	return b
}
