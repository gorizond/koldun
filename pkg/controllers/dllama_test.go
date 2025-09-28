package controllers

import (
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkersForReplicaPower(t *testing.T) {
	tests := []struct {
		name     string
		power    int32
		expected int32
	}{
		{"zero power", 0, 0},
		{"negative power", -1, 0},
		{"power of 1", 1, 1},
		{"power of 2", 2, 3},
		{"power of 3", 3, 5},
		{"power of 10", 10, 19},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := workersForReplicaPower(tt.power)
			if result != tt.expected {
				t.Errorf("workersForReplicaPower(%d) = %d, want %d", tt.power, result, tt.expected)
			}
		})
	}
}

func TestIsModelKind(t *testing.T) {
	tests := []struct {
		name     string
		kind     string
		expected bool
	}{
		{"exact match", "Model", true},
		{"lowercase", "model", true},
		{"mixed case", "MoDel", true},
		{"uppercase", "MODEL", true},
		{"wrong kind", "Pod", false},
		{"empty string", "", false},
		{"whitespace", "  ", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isModelKind(tt.kind)
			if result != tt.expected {
				t.Errorf("isModelKind(%q) = %v, want %v", tt.kind, result, tt.expected)
			}
		})
	}
}

func TestReferencedModelNamespace(t *testing.T) {
	tests := []struct {
		name     string
		dllama   *v1.Dllama
		expected string
	}{
		{
			name: "explicit namespace in modelRef",
			dllama: &v1.Dllama{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "dllama-ns",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Namespace: "model-ns",
					},
				},
			},
			expected: "model-ns",
		},
		{
			name: "no namespace in modelRef, uses dllama namespace",
			dllama: &v1.Dllama{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "dllama-ns",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Namespace: "",
					},
				},
			},
			expected: "dllama-ns",
		},
		{
			name: "empty namespace in modelRef, uses dllama namespace",
			dllama: &v1.Dllama{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{},
				},
			},
			expected: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := referencedModelNamespace(tt.dllama)
			if result != tt.expected {
				t.Errorf("referencedModelNamespace() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestReferencesModel(t *testing.T) {
	tests := []struct {
		name           string
		dllama         *v1.Dllama
		modelNamespace string
		modelName      string
		expected       bool
	}{
		{
			name: "exact match same namespace",
			dllama: &v1.Dllama{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Kind: "Model",
						Name: "llama2-7b",
					},
				},
			},
			modelNamespace: "default",
			modelName:      "llama2-7b",
			expected:       true,
		},
		{
			name: "exact match different namespace",
			dllama: &v1.Dllama{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ai-workloads",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Kind:      "Model",
						Name:      "llama2-7b",
						Namespace: "models",
					},
				},
			},
			modelNamespace: "models",
			modelName:      "llama2-7b",
			expected:       true,
		},
		{
			name: "wrong model name",
			dllama: &v1.Dllama{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Kind: "Model",
						Name: "llama2-7b",
					},
				},
			},
			modelNamespace: "default",
			modelName:      "llama2-13b",
			expected:       false,
		},
		{
			name: "wrong namespace",
			dllama: &v1.Dllama{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Kind: "Model",
						Name: "llama2-7b",
					},
				},
			},
			modelNamespace: "other-namespace",
			modelName:      "llama2-7b",
			expected:       false,
		},
		{
			name: "wrong kind",
			dllama: &v1.Dllama{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Kind: "Pod",
						Name: "llama2-7b",
					},
				},
			},
			modelNamespace: "default",
			modelName:      "llama2-7b",
			expected:       false,
		},
		{
			name: "wrong api group",
			dllama: &v1.Dllama{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						APIGroup: "other.example.com",
						Kind:     "Model",
						Name:     "llama2-7b",
					},
				},
			},
			modelNamespace: "default",
			modelName:      "llama2-7b",
			expected:       false,
		},
		{
			name: "empty model name",
			dllama: &v1.Dllama{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Kind: "Model",
						Name: "",
					},
				},
			},
			modelNamespace: "default",
			modelName:      "llama2-7b",
			expected:       false,
		},
		{
			name:           "nil dllama",
			dllama:         nil,
			modelNamespace: "default",
			modelName:      "llama2-7b",
			expected:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := referencesModel(tt.dllama, tt.modelNamespace, tt.modelName)
			if result != tt.expected {
				t.Errorf("referencesModel() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSplitKey(t *testing.T) {
	tests := []struct {
		name              string
		key               string
		expectedNamespace string
		expectedName      string
	}{
		{"namespace and name", "default/my-resource", "default", "my-resource"},
		{"only name", "my-resource", "", ""},
		{"empty key", "", "", ""},
		{"multiple slashes", "ns/sub/name", "ns", "sub/name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			namespace, name := splitKey(tt.key)
			if namespace != tt.expectedNamespace {
				t.Errorf("splitKey(%q) namespace = %q, want %q", tt.key, namespace, tt.expectedNamespace)
			}
			if name != tt.expectedName {
				t.Errorf("splitKey(%q) name = %q, want %q", tt.key, name, tt.expectedName)
			}
		})
	}
}
