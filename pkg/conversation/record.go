package conversation

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Record is persisted in JetStream KeyValue entries (nats_ttl_<hash>) to describe
// the desired Session topology for a conversation.
type Record struct {
	Hash                    string                `json:"hash"`
	Session                 string                `json:"session,omitempty"`
	Dllama                  string                `json:"dllama,omitempty"`
	Namespace               string                `json:"namespace"`
	Model                   string                `json:"model"`
	CreatedAt               int64                 `json:"createdAt"`
	ReplicaPower            int32                 `json:"replicaPower"`
	RootImage               string                `json:"rootImage"`
	WorkerImage             string                `json:"workerImage"`
	DispatcherImage         string                `json:"dispatcherImage,omitempty"`
	DispatcherMetricsListen string                `json:"dispatcherMetricsListen,omitempty"`
	NATS                    NATSConfig            `json:"nats"`
	Queue                   *QueueConfig          `json:"queue,omitempty"`
	Scaling                 *SessionScalingConfig `json:"scaling,omitempty"`
}

// QueueConfig stores optional NATS backlog configuration for a session.
type QueueConfig struct {
	BacklogSubject        string `json:"backlogSubject,omitempty"`
	ResponseSubjectPrefix string `json:"responseSubjectPrefix,omitempty"`
	AssignmentsBucket     string `json:"assignmentsBucket,omitempty"`
	DllamaSubjectPrefix   string `json:"dllamaSubjectPrefix,omitempty"`
	StateStream           string `json:"stateStream,omitempty"`
}

// SessionScalingConfig stores desired pool sizing parameters for a session.
type SessionScalingConfig struct {
	MinDllamas           int32 `json:"minDllamas,omitempty"`
	MaxDllamas           int32 `json:"maxDllamas,omitempty"`
	ScaleUpBacklog       int32 `json:"scaleUpBacklog,omitempty"`
	ScaleDownIdleSeconds int32 `json:"scaleDownIdleSeconds,omitempty"`
	DesiredDllamas       int32 `json:"desiredDllamas,omitempty"`
	ActiveRequests       int32 `json:"activeRequests,omitempty"`
}

// ParseRecord deserialises and validates a Record payload.
func ParseRecord(data []byte) (*Record, error) {
	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("unmarshal record: %w", err)
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return &record, nil
}

// Marshal serialises the Record into JSON.
func (r *Record) Marshal() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}

// Validate ensures required fields are populated.
func (r *Record) Validate() error {
	if strings.TrimSpace(r.Hash) == "" {
		return fmt.Errorf("hash is required")
	}
	if strings.TrimSpace(r.Session) == "" {
		if strings.TrimSpace(r.Dllama) != "" {
			r.Session = r.Dllama
		} else {
			r.Session = fmt.Sprintf("session-%s", strings.ToLower(strings.TrimSpace(r.Hash)))
		}
	}
	r.Session = sanitizeName(r.Session)
	if strings.TrimSpace(r.Namespace) == "" {
		return fmt.Errorf("namespace is required")
	}
	if strings.TrimSpace(r.Model) == "" {
		return fmt.Errorf("model is required")
	}
	if strings.TrimSpace(r.RootImage) == "" {
		return fmt.Errorf("rootImage is required")
	}
	if strings.TrimSpace(r.WorkerImage) == "" {
		return fmt.Errorf("workerImage is required")
	}
	if strings.TrimSpace(r.DispatcherImage) == "" {
		r.DispatcherImage = r.RootImage
	}
	r.DispatcherMetricsListen = strings.TrimSpace(r.DispatcherMetricsListen)
	if strings.TrimSpace(r.NATS.URL) != "" {
		if err := r.NATS.Validate(); err != nil {
			return fmt.Errorf("nats: %w", err)
		}
	}
	if r.ReplicaPower <= 0 {
		r.ReplicaPower = 1
	}
	if r.Queue == nil {
		r.Queue = &QueueConfig{}
	}
	if r.Queue.ResponseSubjectPrefix != "" && !strings.HasSuffix(r.Queue.ResponseSubjectPrefix, ".") {
		r.Queue.ResponseSubjectPrefix += "."
	}
	if r.Queue.BacklogSubject == "" {
		r.Queue.BacklogSubject = fmt.Sprintf("sessions.%s.requests", strings.ToLower(strings.TrimSpace(r.Hash)))
	}
	if r.Queue.DllamaSubjectPrefix == "" {
		r.Queue.DllamaSubjectPrefix = fmt.Sprintf("sessions.%s.dllama.", strings.ToLower(strings.TrimSpace(r.Hash)))
	} else if !strings.HasSuffix(r.Queue.DllamaSubjectPrefix, ".") {
		r.Queue.DllamaSubjectPrefix += "."
	}
	sanitizedHash := sanitizeIdentifier(strings.ToLower(strings.TrimSpace(r.Hash)))
	if r.Queue.AssignmentsBucket == "" {
		r.Queue.AssignmentsBucket = truncateIdentifier(fmt.Sprintf("sess_%s_assign", sanitizedHash), 63)
	}
	if r.Queue.StateStream == "" {
		r.Queue.StateStream = strings.ToUpper(truncateIdentifier(fmt.Sprintf("sess_%s_state", sanitizedHash), 64))
	}
	if r.Scaling == nil {
		r.Scaling = &SessionScalingConfig{}
	}
	if r.Scaling.MinDllamas <= 0 {
		r.Scaling.MinDllamas = 1
	}
	if r.Scaling.MaxDllamas > 0 && r.Scaling.MaxDllamas < r.Scaling.MinDllamas {
		r.Scaling.MaxDllamas = r.Scaling.MinDllamas
	}
	if r.Scaling.DesiredDllamas < r.Scaling.MinDllamas {
		r.Scaling.DesiredDllamas = r.Scaling.MinDllamas
	}
	if r.Scaling.MaxDllamas > 0 && r.Scaling.DesiredDllamas > r.Scaling.MaxDllamas {
		r.Scaling.DesiredDllamas = r.Scaling.MaxDllamas
	}
	if r.Scaling.ActiveRequests < 0 {
		r.Scaling.ActiveRequests = 0
	}
	return nil
}

// ModelParts splits the stored model identifier into namespace and name.
// The identifier may be either "namespace/name" or just "name" (defaults to the
// conversation namespace).
func (r *Record) ModelParts() (namespace, name string) {
	parts := strings.Split(strings.TrimSpace(r.Model), "/")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}

// NamespacedName returns the canonical <namespace>/<session> string.
func (r *Record) NamespacedName() string {
	return r.SessionNamespacedName()
}

// SessionName returns the computed session resource name.
func (r *Record) SessionName() string {
	if strings.TrimSpace(r.Session) != "" {
		return sanitizeName(r.Session)
	}
	if strings.TrimSpace(r.Dllama) != "" {
		return sanitizeName(r.Dllama)
	}
	if strings.TrimSpace(r.Hash) == "" {
		return ""
	}
	return sanitizeName(fmt.Sprintf("session-%s", strings.ToLower(strings.TrimSpace(r.Hash))))
}

// SessionNamespacedName returns the canonical <namespace>/<session> string.
func (r *Record) SessionNamespacedName() string {
	return fmt.Sprintf("%s/%s", r.Namespace, r.SessionName())
}

// NATSConfig contains connection parameters required by runtime components.
type NATSConfig struct {
	URL string `json:"url"`
	// CredentialsSecret holds the name of the Secret containing credentials (optional).
	CredentialsSecret string `json:"credentialsSecret,omitempty"`
}

// Validate ensures NATSConfig is well-formed.
func (c *NATSConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("missing configuration")
	}
	if strings.TrimSpace(c.URL) == "" {
		return fmt.Errorf("url is required")
	}
	return nil
}

func sanitizeName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return name
	}
	var b strings.Builder
	lastHyphen := false
	for _, r := range name {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if valid {
			b.WriteRune(r)
			lastHyphen = false
			continue
		}
		if r == '-' {
			if lastHyphen {
				continue
			}
			b.WriteRune('-')
			lastHyphen = true
		}
	}
	sanitized := strings.Trim(b.String(), "-")
	if sanitized == "" {
		return name
	}
	if len(sanitized) > 63 {
		sanitized = sanitized[:63]
	}
	return sanitized
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
			b.WriteRune('-')
		}
	}
	return b.String()
}

func truncateIdentifier(value string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(value) <= max {
		return value
	}
	return value[:max]
}
