package controllers

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestIgnoreNotFound tests the ignoreNotFound error handler
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
			name:    "ErrKeyNotFound returns nil",
			err:     nats.ErrKeyNotFound,
			wantNil: true,
		},
		{
			name:    "wrapped ErrKeyNotFound returns nil",
			err:     errors.Join(nats.ErrKeyNotFound, errors.New("context")),
			wantNil: true,
		},
		{
			name:    "other error returns error",
			err:     errors.New("some other error"),
			wantNil: false,
		},
		{
			name:    "NATS timeout error returns error",
			err:     nats.ErrTimeout,
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ignoreNotFound(tt.err)
			if tt.wantNil && got != nil {
				t.Errorf("ignoreNotFound(%v) = %v, want nil", tt.err, got)
			}
			if !tt.wantNil && got == nil {
				t.Errorf("ignoreNotFound(%v) = nil, want error", tt.err)
			}
		})
	}
}

// TestModelKey tests the modelKey function which creates namespaced keys
func TestModelKey(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		modelName string
		want      string
	}{
		{
			name:      "normal namespace and name",
			namespace: "default",
			modelName: "llama-7b",
			want:      "default/llama-7b",
		},
		{
			name:      "empty namespace defaults to 'default'",
			namespace: "",
			modelName: "llama-7b",
			want:      "default/llama-7b",
		},
		{
			name:      "whitespace namespace defaults to 'default'",
			namespace: "   ",
			modelName: "llama-7b",
			want:      "default/llama-7b",
		},
		{
			name:      "custom namespace",
			namespace: "ai-models",
			modelName: "gpt-4",
			want:      "ai-models/gpt-4",
		},
		{
			name:      "name with spaces trimmed",
			namespace: "default",
			modelName: "  llama-7b  ",
			want:      "default/llama-7b",
		},
		{
			name:      "both with spaces trimmed",
			namespace: "  ai-models  ",
			modelName: "  gpt-4  ",
			want:      "ai-models/gpt-4",
		},
		{
			name:      "empty name",
			namespace: "default",
			modelName: "",
			want:      "default/",
		},
		{
			name:      "name with dashes and numbers",
			namespace: "prod",
			modelName: "llama-2-7b-chat",
			want:      "prod/llama-2-7b-chat",
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

// TestModelReady tests the modelReady function which checks if a model is ready
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
			name: "model with empty OutputPVCName is not ready",
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
			name: "model with whitespace OutputPVCName is not ready",
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
			name: "model with zero size bytes and empty human size is not ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "pvc-test",
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
			name: "model with negative size bytes and empty human size is not ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "pvc-test",
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
			name: "model with Ready condition false is not ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "pvc-test",
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
			name: "model with no Ready condition is not ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "pvc-test",
					ConversionSizeBytes: 1000,
					ConversionSizeHuman: "1KB",
					Conditions:          []metav1.Condition{},
				},
			},
			want: false,
		},
		{
			name: "fully ready model with size bytes",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "pvc-test",
					ConversionSizeBytes: 1000000,
					ConversionSizeHuman: "1MB",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: true,
		},
		{
			name: "ready model with only human size",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "pvc-test",
					ConversionSizeBytes: 0,
					ConversionSizeHuman: "1MB",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: true,
		},
		{
			name: "ready model with only size bytes",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "pvc-test",
					ConversionSizeBytes: 1000000,
					ConversionSizeHuman: "",
					Conditions: []metav1.Condition{
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: true,
		},
		{
			name: "ready model with multiple conditions",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "pvc-test",
					ConversionSizeBytes: 1000000,
					ConversionSizeHuman: "1MB",
					Conditions: []metav1.Condition{
						{Type: "Downloaded", Status: metav1.ConditionTrue},
						{Type: "Converted", Status: metav1.ConditionTrue},
						{Type: conditionReady, Status: metav1.ConditionTrue},
					},
				},
			},
			want: true,
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
