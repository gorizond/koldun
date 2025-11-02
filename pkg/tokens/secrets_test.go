package tokens

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIsTokenSecret(t *testing.T) {
	tests := []struct {
		name   string
		secret *corev1.Secret
		want   bool
	}{
		{
			name:   "nil secret",
			secret: nil,
			want:   false,
		},
		{
			name: "no token label",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			want: false,
		},
		{
			name: "token label true",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						LabelToken: "true",
					},
				},
			},
			want: true,
		},
		{
			name: "token label 1",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						LabelToken: "1",
					},
				},
			},
			want: true,
		},
		{
			name: "token label yes",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						LabelToken: "yes",
					},
				},
			},
			want: true,
		},
		{
			name: "token label false",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						LabelToken: "false",
					},
				},
			},
			want: false,
		},
		{
			name: "token label invalid",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						LabelToken: "invalid",
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsTokenSecret(tt.secret)
			if got != tt.want {
				t.Errorf("IsTokenSecret() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHash(t *testing.T) {
	tests := []struct {
		name   string
		secret *corev1.Secret
		want   string
	}{
		{
			name:   "nil secret",
			secret: nil,
			want:   "",
		},
		{
			name: "no hash data",
			secret: &corev1.Secret{
				Data: map[string][]byte{},
			},
			want: "",
		},
		{
			name: "valid hash",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					DataHashKey: []byte("abc123def456"),
				},
			},
			want: "abc123def456",
		},
		{
			name: "hash with whitespace",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					DataHashKey: []byte("  ABC123DEF456  "),
				},
			},
			want: "abc123def456",
		},
		{
			name: "uppercase hash normalized",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					DataHashKey: []byte("ABC123DEF456"),
				},
			},
			want: "abc123def456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Hash(tt.secret)
			if got != tt.want {
				t.Errorf("Hash() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractRegistryToken(t *testing.T) {
	tests := []struct {
		name    string
		secret  *corev1.Secret
		wantErr bool
	}{
		{
			name:    "nil secret",
			secret:  nil,
			wantErr: true,
		},
		{
			name: "missing hash",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-token",
					Namespace: "default",
				},
				Data: map[string][]byte{},
			},
			wantErr: true,
		},
		{
			name: "valid minimal token",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-token",
					Namespace: "default",
				},
				Data: map[string][]byte{
					DataHashKey: []byte("abc123"),
				},
			},
			wantErr: false,
		},
		{
			name: "token with disabled flag in data",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-token",
					Namespace: "default",
				},
				Data: map[string][]byte{
					DataHashKey:     []byte("abc123"),
					DataDisabledKey: []byte("true"),
				},
			},
			wantErr: false,
		},
		{
			name: "token with disabled annotation",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-token",
					Namespace: "default",
					Annotations: map[string]string{
						AnnotationDisabled: "yes",
					},
				},
				Data: map[string][]byte{
					DataHashKey: []byte("abc123"),
				},
			},
			wantErr: false,
		},
		{
			name: "token with metadata JSON",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-token",
					Namespace: "default",
				},
				Data: map[string][]byte{
					DataHashKey:     []byte("abc123"),
					DataMetadataKey: []byte(`{"user":"alice","role":"admin"}`),
				},
			},
			wantErr: false,
		},
		{
			name: "token with metadata annotations",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-token",
					Namespace: "default",
					Annotations: map[string]string{
						AnnotationMetadataPrefix + "user": "alice",
						AnnotationMetadataPrefix + "role": "admin",
					},
				},
				Data: map[string][]byte{
					DataHashKey: []byte("abc123"),
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := ExtractRegistryToken(tt.secret)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractRegistryToken() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && token == nil {
				t.Error("ExtractRegistryToken() returned nil token")
			}
			if !tt.wantErr && token != nil {
				if token.Hash == "" {
					t.Error("Token hash should not be empty")
				}
				if token.Namespace != tt.secret.Namespace {
					t.Errorf("Token namespace = %v, want %v", token.Namespace, tt.secret.Namespace)
				}
			}
		})
	}
}

func TestExtractRegistryToken_DisabledFlag(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-token",
			Namespace: "default",
		},
		Data: map[string][]byte{
			DataHashKey:     []byte("abc123"),
			DataDisabledKey: []byte("true"),
		},
	}

	token, err := ExtractRegistryToken(secret)
	if err != nil {
		t.Fatalf("ExtractRegistryToken() error = %v", err)
	}

	if !token.Disabled {
		t.Error("Token should be disabled")
	}
}

func TestExtractRegistryToken_Metadata(t *testing.T) {
	metadata := map[string]string{
		"user": "alice",
		"role": "admin",
	}
	metadataJSON, _ := json.Marshal(metadata)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-token",
			Namespace: "default",
		},
		Data: map[string][]byte{
			DataHashKey:     []byte("abc123"),
			DataMetadataKey: metadataJSON,
		},
	}

	token, err := ExtractRegistryToken(secret)
	if err != nil {
		t.Fatalf("ExtractRegistryToken() error = %v", err)
	}

	if token.Metadata == nil {
		t.Fatal("Token metadata should not be nil")
	}

	if token.Metadata["user"] != "alice" {
		t.Errorf("Token metadata user = %v, want alice", token.Metadata["user"])
	}

	if token.Metadata["role"] != "admin" {
		t.Errorf("Token metadata role = %v, want admin", token.Metadata["role"])
	}
}

func TestParseLabelBool(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "empty", value: "", want: false},
		{name: "true", value: "true", want: true},
		{name: "TRUE", value: "TRUE", want: true},
		{name: "1", value: "1", want: true},
		{name: "yes", value: "yes", want: true},
		{name: "YES", value: "YES", want: true},
		{name: "y", value: "y", want: true},
		{name: "on", value: "on", want: true},
		{name: "false", value: "false", want: false},
		{name: "FALSE", value: "FALSE", want: false},
		{name: "0", value: "0", want: false},
		{name: "no", value: "no", want: false},
		{name: "NO", value: "NO", want: false},
		{name: "n", value: "n", want: false},
		{name: "off", value: "off", want: false},
		{name: "invalid", value: "invalid", want: false},
		{name: "whitespace", value: "  true  ", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseLabelBool(tt.value)
			if got != tt.want {
				t.Errorf("parseLabelBool(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestNormalise(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "empty", value: "", want: ""},
		{name: "lowercase", value: "test", want: "test"},
		{name: "uppercase", value: "TEST", want: "test"},
		{name: "mixed case", value: "TeSt", want: "test"},
		{name: "with spaces", value: "  test  ", want: "test"},
		{name: "mixed case with spaces", value: "  TeSt  ", want: "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalise(tt.value)
			if got != tt.want {
				t.Errorf("normalise(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestDisabledFromSecret(t *testing.T) {
	tests := []struct {
		name    string
		secret  *corev1.Secret
		want    bool
		wantErr bool
	}{
		{
			name:    "nil secret",
			secret:  nil,
			want:    false,
			wantErr: false,
		},
		{
			name: "no disabled flag",
			secret: &corev1.Secret{
				Data: map[string][]byte{},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "disabled in data - true",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					DataDisabledKey: []byte("true"),
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "disabled in annotation - yes",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						AnnotationDisabled: "yes",
					},
				},
				Data: map[string][]byte{},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "invalid disabled value",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					DataDisabledKey: []byte("invalid"),
				},
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "data takes precedence over annotation",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						AnnotationDisabled: "yes",
					},
				},
				Data: map[string][]byte{
					DataDisabledKey: []byte("false"),
				},
			},
			want:    false,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := disabledFromSecret(tt.secret)
			if (err != nil) != tt.wantErr {
				t.Errorf("disabledFromSecret() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("disabledFromSecret() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsHashJetStreamSafe(t *testing.T) {
	tests := []struct {
		name string
		hash string
		ok   bool
	}{
		{name: "empty", hash: "", ok: false},
		{name: "lowercase hex", hash: "abc123", ok: true},
		{name: "with dash", hash: "abc-123", ok: true},
		{name: "with underscore", hash: "abc_123", ok: true},
		{name: "uppercase rejected", hash: "ABC123", ok: false},
		{name: "slash rejected", hash: "abc/123", ok: false},
		{name: "colon rejected", hash: "abc:123", ok: false},
		{name: "plus rejected", hash: "abc+123", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsHashJetStreamSafe(tt.hash); got != tt.ok {
				t.Errorf("IsHashJetStreamSafe(%q) = %v, want %v", tt.hash, got, tt.ok)
			}
		})
	}
}

func TestMetadataFromSecret(t *testing.T) {
	tests := []struct {
		name    string
		secret  *corev1.Secret
		want    map[string]string
		wantErr bool
	}{
		{
			name:    "nil secret",
			secret:  nil,
			want:    nil,
			wantErr: false,
		},
		{
			name: "no metadata",
			secret: &corev1.Secret{
				Data: map[string][]byte{},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "metadata from JSON",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					DataMetadataKey: []byte(`{"user":"alice","role":"admin"}`),
				},
			},
			want: map[string]string{
				"user": "alice",
				"role": "admin",
			},
			wantErr: false,
		},
		{
			name: "metadata from annotations",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						AnnotationMetadataPrefix + "user": "bob",
						AnnotationMetadataPrefix + "team": "engineering",
					},
				},
				Data: map[string][]byte{},
			},
			want: map[string]string{
				"user": "bob",
				"team": "engineering",
			},
			wantErr: false,
		},
		{
			name: "combined metadata",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						AnnotationMetadataPrefix + "team": "engineering",
					},
				},
				Data: map[string][]byte{
					DataMetadataKey: []byte(`{"user":"alice"}`),
				},
			},
			want: map[string]string{
				"user": "alice",
				"team": "engineering",
			},
			wantErr: false,
		},
		{
			name: "invalid JSON metadata",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					DataMetadataKey: []byte(`invalid json`),
				},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := metadataFromSecret(tt.secret)
			if (err != nil) != tt.wantErr {
				t.Errorf("metadataFromSecret() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(got) != len(tt.want) {
					t.Errorf("metadataFromSecret() length = %v, want %v", len(got), len(tt.want))
				}
				for k, v := range tt.want {
					if got[k] != v {
						t.Errorf("metadataFromSecret()[%s] = %v, want %v", k, got[k], v)
					}
				}
			}
		})
	}
}
