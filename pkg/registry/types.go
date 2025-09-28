package registry

const (
	DefaultModelBucket = "koldun_models"
	DefaultTokenBucket = "koldun_tokens"
	DefaultModelPrefix = "model/"
	DefaultTokenPrefix = "token/"
)

// Model represents the payload published in the NATS registry bucket for ready models.
type Model struct {
	Namespace           string `json:"namespace"`
	Name                string `json:"name"`
	DisplayName         string `json:"displayName,omitempty"`
	ConversionSizeBytes int64  `json:"conversionSizeBytes,omitempty"`
	ConversionSizeHuman string `json:"conversionSizeHuman,omitempty"`
	OutputPVCName       string `json:"outputPVCName,omitempty"`
	ReplicaPower        int32  `json:"replicaPower,omitempty"`
}

// Token represents an API token entry stored in the registry bucket.
type Token struct {
	Hash      string            `json:"hash"`
	Disabled  bool              `json:"disabled"`
	Namespace string            `json:"namespace"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}
