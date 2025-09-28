package conversation

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Record is persisted in JetStream KeyValue entries (nats_ttl_<hash>) to describe
// the desired Dllama topology for a conversation.
type Record struct {
	Hash         string     `json:"hash"`
	Dllama       string     `json:"dllama"`
	Namespace    string     `json:"namespace"`
	Model        string     `json:"model"`
	CreatedAt    int64      `json:"createdAt"`
	ReplicaPower int32      `json:"replicaPower"`
	RootImage    string     `json:"rootImage"`
	WorkerImage  string     `json:"workerImage"`
	NATS         NATSConfig `json:"nats"`
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
	if strings.TrimSpace(r.Dllama) == "" {
		return fmt.Errorf("dllama is required")
	}
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
	if strings.TrimSpace(r.NATS.URL) != "" {
		if err := r.NATS.Validate(); err != nil {
			return fmt.Errorf("nats: %w", err)
		}
	}
	if r.ReplicaPower <= 0 {
		r.ReplicaPower = 1
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

// NamespacedName returns the canonical <namespace>/<dllama> string.
func (r *Record) NamespacedName() string {
	return fmt.Sprintf("%s/%s", r.Namespace, r.Dllama)
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
