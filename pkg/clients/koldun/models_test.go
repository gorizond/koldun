package koldun

import (
	"context"
	"strings"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

func TestNewModelClient(t *testing.T) {
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
			client, err := NewModelClient(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewModelClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && client == nil {
				t.Error("NewModelClient() returned nil client without error")
			}
		})
	}
}

func TestModelClient_List(t *testing.T) {
	scheme := runtime.NewScheme()
	v1.AddToScheme(scheme)

	model1 := &v1.Model{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       "Model",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "model1",
			Namespace: "test-ns",
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://huggingface.co/mistralai/Mistral-7B-v0.3",
			LocalPath: "/models/mistral",
		},
	}

	model2 := &v1.Model{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       "Model",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "model2",
			Namespace: "test-ns",
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://huggingface.co/gpt2",
			LocalPath: "/models/gpt2",
		},
	}

	// Convert models to unstructured
	u1 := &unstructured.Unstructured{}
	u2 := &unstructured.Unstructured{}
	u1Data, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(model1)
	u1.Object = u1Data
	u2Data, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(model2)
	u2.Object = u2Data

	tests := []struct {
		name      string
		namespace string
		objects   []runtime.Object
		wantLen   int
		wantErr   bool
	}{
		{
			name:      "list in namespace",
			namespace: "test-ns",
			objects:   []runtime.Object{u1, u2},
			wantLen:   2,
			wantErr:   false,
		},
		{
			name:      "list all namespaces",
			namespace: "",
			objects:   []runtime.Object{u1, u2},
			wantLen:   2,
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
			// Create fake dynamic client
			dynamicClient := fake.NewSimpleDynamicClient(scheme, tt.objects...)

			// Create model client with fake dynamic client
			client := &ModelClient{
				resource: dynamicClient.Resource(modelGVR),
			}

			models, err := client.List(context.Background(), tt.namespace)
			if (err != nil) != tt.wantErr {
				t.Errorf("ModelClient.List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(models) != tt.wantLen {
				t.Errorf("ModelClient.List() returned %d models, want %d", len(models), tt.wantLen)
			}
		})
	}
}

func TestModelClient_Get(t *testing.T) {
	scheme := runtime.NewScheme()
	v1.AddToScheme(scheme)

	model := &v1.Model{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       "Model",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-model",
			Namespace: "test-ns",
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://huggingface.co/mistralai/Mistral-7B-v0.3",
			LocalPath: "/models/mistral",
		},
	}

	// Convert model to unstructured
	u := &unstructured.Unstructured{}
	uData, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(model)
	u.Object = uData

	tests := []struct {
		name      string
		namespace string
		modelName string
		objects   []runtime.Object
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "get existing model",
			namespace: "test-ns",
			modelName: "test-model",
			objects:   []runtime.Object{u},
			wantErr:   false,
		},
		{
			name:      "model not found",
			namespace: "test-ns",
			modelName: "nonexistent",
			objects:   []runtime.Object{u},
			wantErr:   true,
			errMsg:    "not found",
		},
		{
			name:      "empty namespace",
			namespace: "",
			modelName: "test-model",
			objects:   []runtime.Object{u},
			wantErr:   true,
			errMsg:    "namespace is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake dynamic client
			dynamicClient := fake.NewSimpleDynamicClient(scheme, tt.objects...)

			// Create model client with fake dynamic client
			client := &ModelClient{
				resource: dynamicClient.Resource(modelGVR),
			}

			model, err := client.Get(context.Background(), tt.namespace, tt.modelName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ModelClient.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errMsg != "" && err != nil {
				if !contains(err.Error(), tt.errMsg) {
					t.Errorf("ModelClient.Get() error = %v, want error containing %q", err, tt.errMsg)
				}
			}
			if !tt.wantErr && model == nil {
				t.Error("ModelClient.Get() returned nil model without error")
			}
		})
	}
}

func TestToModel(t *testing.T) {
	tests := []struct {
		name    string
		input   *unstructured.Unstructured
		wantErr bool
	}{
		{
			name: "valid model conversion",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": v1.SchemeGroupVersion.String(),
					"kind":       "Model",
					"metadata": map[string]interface{}{
						"name":      "test-model",
						"namespace": "test-ns",
					},
					"spec": map[string]interface{}{
						"sourceUrl": "https://huggingface.co/mistralai/Mistral-7B-v0.3",
						"localPath": "/models/mistral",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid model structure",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": v1.SchemeGroupVersion.String(),
					"kind":       "Model",
					"spec": map[string]interface{}{
						"sourceUrl": 123, // Wrong type
					},
				},
			},
			wantErr: true,
		},
		{
			name: "empty unstructured",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model, err := toModel(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("toModel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && model == nil {
				t.Error("toModel() returned nil model without error")
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
