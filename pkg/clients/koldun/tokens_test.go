package koldun

import (
	"context"
	"strings"
	"testing"

	"github.com/gorizond/koldun/pkg/registry"
	"github.com/gorizond/koldun/pkg/tokens"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

func TestNewTokenClient(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *rest.Config
		wantErr bool
	}{
		{
			name:    "valid config",
			cfg:     &rest.Config{Host: "http://localhost:8080"},
			wantErr: false,
		},
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewTokenClient(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewTokenClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && client == nil {
				t.Error("NewTokenClient() returned nil client without error")
			}
		})
	}
}

func TestTokenClient_List(t *testing.T) {
	tokenSecret1 := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "token1",
			Namespace: "test-ns",
			Labels: map[string]string{
				tokens.LabelToken: "true",
			},
			Annotations: map[string]string{
				"metadata": `{"name":"test-token-1","disabled":false}`,
			},
		},
		Data: map[string][]byte{
			"token": []byte("sk-test-token-1"),
			"hash":  []byte("test-hash-1"),
		},
	}

	tokenSecret2 := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "token2",
			Namespace: "test-ns",
			Labels: map[string]string{
				tokens.LabelToken: "true",
			},
		},
		Data: map[string][]byte{
			"token": []byte("sk-test-token-2"),
			"hash":  []byte("test-hash-2"),
		},
	}

	// Non-token secret (should be filtered out)
	nonTokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "not-a-token",
			Namespace: "test-ns",
		},
		Data: map[string][]byte{
			"key": []byte("value"),
		},
	}

	tests := []struct {
		name      string
		namespace string
		objects   []runtime.Object
		wantLen   int
		wantErr   bool
	}{
		{
			name:      "list tokens in namespace",
			namespace: "test-ns",
			objects:   []runtime.Object{tokenSecret1, tokenSecret2, nonTokenSecret},
			wantLen:   2,
			wantErr:   false,
		},
		{
			name:      "list tokens all namespaces",
			namespace: "",
			objects:   []runtime.Object{tokenSecret1, tokenSecret2},
			wantLen:   2,
			wantErr:   false,
		},
		{
			name:      "empty namespace string with spaces",
			namespace: "  ",
			objects:   []runtime.Object{tokenSecret1},
			wantLen:   1,
			wantErr:   false,
		},
		{
			name:      "no tokens found",
			namespace: "test-ns",
			objects:   []runtime.Object{nonTokenSecret},
			wantLen:   0,
			wantErr:   false,
		},
		{
			name:      "empty list",
			namespace: "test-ns",
			objects:   []runtime.Object{},
			wantLen:   0,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset(tt.objects...)
			client := &TokenClient{client: fakeClient}

			tokens, err := client.List(context.Background(), tt.namespace)
			if (err != nil) != tt.wantErr {
				t.Errorf("TokenClient.List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(tokens) != tt.wantLen {
				t.Errorf("TokenClient.List() returned %d tokens, want %d", len(tokens), tt.wantLen)
			}
		})
	}

	// Test nil client
	t.Run("nil client", func(t *testing.T) {
		var client *TokenClient
		_, err := client.List(context.Background(), "test-ns")
		if err == nil || !strings.Contains(err.Error(), "not initialised") {
			t.Errorf("TokenClient.List() with nil client error = %v, want 'not initialised'", err)
		}
	})

	// Test client with nil kubernetes client
	t.Run("nil kubernetes client", func(t *testing.T) {
		client := &TokenClient{client: nil}
		_, err := client.List(context.Background(), "test-ns")
		if err == nil || !strings.Contains(err.Error(), "not initialised") {
			t.Errorf("TokenClient.List() with nil kubernetes client error = %v, want 'not initialised'", err)
		}
	})
}

func TestTokenClient_Get(t *testing.T) {
	tokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-token",
			Namespace: "test-ns",
			Labels: map[string]string{
				tokens.LabelToken: "true",
			},
			Annotations: map[string]string{
				"metadata": `{"name":"test-token","disabled":false}`,
			},
		},
		Data: map[string][]byte{
			"token": []byte("sk-test-token"),
			"hash":  []byte("test-hash"),
		},
	}

	nonTokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "not-a-token",
			Namespace: "test-ns",
		},
		Data: map[string][]byte{
			"key": []byte("value"),
		},
	}

	tests := []struct {
		name       string
		namespace  string
		secretName string
		objects    []runtime.Object
		wantErr    bool
		errMsg     string
	}{
		{
			name:       "get existing token",
			namespace:  "test-ns",
			secretName: "test-token",
			objects:    []runtime.Object{tokenSecret},
			wantErr:    false,
		},
		{
			name:       "token not found",
			namespace:  "test-ns",
			secretName: "nonexistent",
			objects:    []runtime.Object{tokenSecret},
			wantErr:    true,
			errMsg:     "not found",
		},
		{
			name:       "secret exists but not a token",
			namespace:  "test-ns",
			secretName: "not-a-token",
			objects:    []runtime.Object{nonTokenSecret},
			wantErr:    true,
			errMsg:     "not labelled as a koldun token",
		},
		{
			name:       "empty namespace",
			namespace:  "",
			secretName: "test-token",
			objects:    []runtime.Object{tokenSecret},
			wantErr:    true,
			errMsg:     "namespace is required",
		},
		{
			name:       "namespace with only spaces",
			namespace:  "   ",
			secretName: "test-token",
			objects:    []runtime.Object{tokenSecret},
			wantErr:    true,
			errMsg:     "namespace is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset(tt.objects...)
			client := &TokenClient{client: fakeClient}

			token, err := client.Get(context.Background(), tt.namespace, tt.secretName)
			if (err != nil) != tt.wantErr {
				t.Errorf("TokenClient.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errMsg != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("TokenClient.Get() error = %v, want error containing %q", err, tt.errMsg)
				}
			}
			if !tt.wantErr && token == nil {
				t.Error("TokenClient.Get() returned nil token without error")
			}
		})
	}

	// Test nil client
	t.Run("nil client", func(t *testing.T) {
		var client *TokenClient
		_, err := client.Get(context.Background(), "test-ns", "test-token")
		if err == nil || !strings.Contains(err.Error(), "not initialised") {
			t.Errorf("TokenClient.Get() with nil client error = %v, want 'not initialised'", err)
		}
	})

	// Test client with nil kubernetes client
	t.Run("nil kubernetes client", func(t *testing.T) {
		client := &TokenClient{client: nil}
		_, err := client.Get(context.Background(), "test-ns", "test-token")
		if err == nil || !strings.Contains(err.Error(), "not initialised") {
			t.Errorf("TokenClient.Get() with nil kubernetes client error = %v, want 'not initialised'", err)
		}
	})
}

func TestTokenClient_ListWithError(t *testing.T) {
	// Test API error during list
	fakeClient := fake.NewSimpleClientset()
	fakeClient.PrependReactor("list", "secrets", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, &fakeAPIError{message: "API error"}
	})

	client := &TokenClient{client: fakeClient}
	_, err := client.List(context.Background(), "test-ns")
	if err == nil || !strings.Contains(err.Error(), "list token secrets") {
		t.Errorf("TokenClient.List() with API error = %v, want error containing 'list token secrets'", err)
	}
}

func TestTokenClient_GetWithError(t *testing.T) {
	// Test API error during get
	fakeClient := fake.NewSimpleClientset()
	fakeClient.PrependReactor("get", "secrets", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, &fakeAPIError{message: "API error"}
	})

	client := &TokenClient{client: fakeClient}
	_, err := client.Get(context.Background(), "test-ns", "test-token")
	if err == nil || !strings.Contains(err.Error(), "get token secret") {
		t.Errorf("TokenClient.Get() with API error = %v, want error containing 'get token secret'", err)
	}
}

func TestTokenExtraction(t *testing.T) {
	// Test that our code properly handles token extraction
	tests := []struct {
		name    string
		secret  *corev1.Secret
		want    *registry.Token
		wantErr bool
	}{
		{
			name: "valid token secret",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "token",
					Namespace: "ns",
					Labels: map[string]string{
						tokens.LabelToken: "true",
					},
				},
				Data: map[string][]byte{
					"token": []byte("sk-test"),
					"hash":  []byte("hash123"),
				},
			},
			want: &registry.Token{
				Hash:      "hash123",
				Disabled:  false,
				Namespace: "ns",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tokens.IsTokenSecret(tt.secret) {
				t.Error("Expected secret to be identified as token secret")
			}

			got, err := tokens.ExtractRegistryToken(tt.secret)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractRegistryToken() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if got.Hash != tt.want.Hash {
					t.Errorf("ExtractRegistryToken() hash = %v, want %v", got.Hash, tt.want.Hash)
				}
				if got.Disabled != tt.want.Disabled {
					t.Errorf("ExtractRegistryToken() disabled = %v, want %v", got.Disabled, tt.want.Disabled)
				}
			}
		})
	}
}

// fakeAPIError implements error interface for testing
type fakeAPIError struct {
	message string
}

func (e *fakeAPIError) Error() string {
	return e.message
}
