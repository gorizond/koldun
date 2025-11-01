package controllers

import (
	"errors"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/nats-io/nats.go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestIgnoreNotFound tests the ignoreNotFound function which filters out
// NATS ErrKeyNotFound errors while preserving other errors.
func TestIgnoreNotFound(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantNil bool
	}{
		{
			name:    "nil error returns nil",
			err:     nil,
			wantNil: true,
		},
		{
			name:    "NATS ErrKeyNotFound returns nil",
			err:     nats.ErrKeyNotFound,
			wantNil: true,
		},
		{
			name:    "wrapped ErrKeyNotFound returns nil",
			err:     errors.New("some context: " + nats.ErrKeyNotFound.Error()),
			wantNil: false, // errors.Is won't match wrapped string
		},
		{
			name:    "other error is preserved",
			err:     errors.New("connection failed"),
			wantNil: false,
		},
		{
			name:    "nats timeout error is preserved",
			err:     nats.ErrTimeout,
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ignoreNotFound(tt.err)
			if tt.wantNil && got != nil {
				t.Errorf("ignoreNotFound() = %v, want nil", got)
			}
			if !tt.wantNil && got == nil {
				t.Errorf("ignoreNotFound() = nil, want error")
			}
			if !tt.wantNil && got != tt.err {
				t.Errorf("ignoreNotFound() = %v, want %v", got, tt.err)
			}
		})
	}
}

// TestModelKey tests the modelKey function which generates namespaced keys
// for model storage in NATS KV.
func TestModelKey(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		modelName string
		want      string
	}{
		{
			name:      "normal namespace and name",
			namespace: "production",
			modelName: "llama-7b",
			want:      "production/llama-7b",
		},
		{
			name:      "empty namespace defaults to default",
			namespace: "",
			modelName: "gpt2",
			want:      "default/gpt2",
		},
		{
			name:      "whitespace namespace defaults to default",
			namespace: "  ",
			modelName: "bert",
			want:      "default/bert",
		},
		{
			name:      "namespace with whitespace is trimmed",
			namespace: "  staging  ",
			modelName: "model-v2",
			want:      "staging/model-v2",
		},
		{
			name:      "model name with whitespace is trimmed",
			namespace: "dev",
			modelName: "  my-model  ",
			want:      "dev/my-model",
		},
		{
			name:      "both trimmed",
			namespace: " kube-system ",
			modelName: " metrics-server ",
			want:      "kube-system/metrics-server",
		},
		{
			name:      "default namespace",
			namespace: "default",
			modelName: "test-model",
			want:      "default/test-model",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := modelKey(tt.namespace, tt.modelName)
			if got != tt.want {
				t.Errorf("modelKey(%q, %q) = %q, want %q", tt.namespace, tt.modelName, got, tt.want)
			}
		})
	}
}

// TestModelReady tests the modelReady function which determines if a Model
// is ready for use based on its status fields and conditions.
func TestModelReady(t *testing.T) {
	tests := []struct {
		name  string
		model *v1.Model
		want  bool
	}{
		{
			name:  "nil model is not ready",
			model: nil,
			want:  false,
		},
		{
			name: "empty OutputPVCName is not ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "",
					ConversionSizeBytes: 1000,
					ConversionSizeHuman: "1KB",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: false,
		},
		{
			name: "whitespace OutputPVCName is not ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "   ",
					ConversionSizeBytes: 1000,
					ConversionSizeHuman: "1KB",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: false,
		},
		{
			name: "missing size information is not ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "model-pvc",
					ConversionSizeBytes: 0,
					ConversionSizeHuman: "",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: false,
		},
		{
			name: "negative size bytes is not ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "model-pvc",
					ConversionSizeBytes: -100,
					ConversionSizeHuman: "",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: false,
		},
		{
			name: "ready condition false is not ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "model-pvc",
					ConversionSizeBytes: 1000,
					ConversionSizeHuman: "1KB",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionFalse},
					},
				},
			},
			want: false,
		},
		{
			name: "missing ready condition is not ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "model-pvc",
					ConversionSizeBytes: 1000,
					ConversionSizeHuman: "1KB",
					Conditions:          []metav1.Condition{},
				},
			},
			want: false,
		},
		{
			name: "fully ready model with bytes only",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "model-pvc",
					ConversionSizeBytes: 5000000000,
					ConversionSizeHuman: "",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: true,
		},
		{
			name: "fully ready model with human size only",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "llama-7b-pvc",
					ConversionSizeBytes: 0,
					ConversionSizeHuman: "5GB",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: true,
		},
		{
			name: "fully ready model with both sizes",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "gpt2-medium-pvc",
					ConversionSizeBytes: 5000000000,
					ConversionSizeHuman: "5GB",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: true,
		},
		{
			name: "ready with multiple conditions",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "model-pvc",
					ConversionSizeBytes: 1000,
					Conditions: []metav1.Condition{
						{Type: "Downloaded", Status: metav1.ConditionTrue},
						{Type: "Converted", Status: metav1.ConditionTrue},
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: true,
		},
		{
			name: "ready condition unknown is not ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "model-pvc",
					ConversionSizeBytes: 1000,
					ConversionSizeHuman: "1KB",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionUnknown},
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := modelReady(tt.model)
			if got != tt.want {
				t.Errorf("modelReady() = %v, want %v", got, tt.want)
			}
		})
	}
}
