package controllers

import (
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

func TestResourceSessionKey(t *testing.T) {
	tests := []struct {
		name      string
		resource  string
		namespace string
		objName   string
		expected  string
	}{
		{
			name:      "valid key",
			resource:  "dllama",
			namespace: "default",
			objName:   "test-dllama-abc",
			expected:  "dllama/default/test-dllama-abc",
		},
		{
			name:      "empty resource",
			resource:  "",
			namespace: "default",
			objName:   "test",
			expected:  "",
		},
		{
			name:      "empty name",
			resource:  "dllama",
			namespace: "default",
			objName:   "",
			expected:  "",
		},
		{
			name:      "empty namespace is allowed",
			resource:  "root",
			namespace: "",
			objName:   "test-root",
			expected:  "root//test-root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := resourceSessionKey(tt.resource, tt.namespace, tt.objName)
			if result != tt.expected {
				t.Errorf("resourceSessionKey(%q, %q, %q) = %q, want %q",
					tt.resource, tt.namespace, tt.objName, result, tt.expected)
			}
		})
	}
}

func TestSplitNamespaceName(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		expectedNS   string
		expectedName string
	}{
		{
			name:         "valid namespaced key",
			key:          "default/my-resource",
			expectedNS:   "default",
			expectedName: "my-resource",
		},
		{
			name:         "name only - no namespace",
			key:          "my-resource",
			expectedNS:   "",
			expectedName: "my-resource",
		},
		{
			name:         "empty key",
			key:          "",
			expectedNS:   "",
			expectedName: "",
		},
		{
			name:         "multiple slashes - only first split",
			key:          "kube-system/pod/container",
			expectedNS:   "kube-system",
			expectedName: "pod/container",
		},
		{
			name:         "empty namespace part",
			key:          "/my-resource",
			expectedNS:   "",
			expectedName: "my-resource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, name := splitNamespaceName(tt.key)
			if ns != tt.expectedNS {
				t.Errorf("namespace: got %q, want %q", ns, tt.expectedNS)
			}
			if name != tt.expectedName {
				t.Errorf("name: got %q, want %q", name, tt.expectedName)
			}
		})
	}
}

func TestGuessSessionFromDllamaName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "valid dllama name",
			input:    "my-session-dllama-abc123",
			expected: "my-session",
		},
		{
			name:     "session with hyphens",
			input:    "prod-user-123-dllama-xyz",
			expected: "prod-user-123",
		},
		{
			name:     "no -dllama suffix",
			input:    "my-session-abc",
			expected: "",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "-dllama at start (invalid)",
			input:    "-dllama-abc",
			expected: "",
		},
		{
			name:     "multiple -dllama occurrences - use first",
			input:    "session-dllama-test-dllama-abc",
			expected: "session",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := guessSessionFromDllamaName(tt.input)
			if result != tt.expected {
				t.Errorf("guessSessionFromDllamaName(%q) = %q, want %q",
					tt.input, result, tt.expected)
			}
		})
	}
}

func TestGuessSessionFromRootName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "valid root name",
			input:    "my-session-dllama-abc-root",
			expected: "my-session",
		},
		{
			name:     "root name without -root suffix",
			input:    "my-session-dllama-abc",
			expected: "my-session",
		},
		{
			name:     "no dllama in name",
			input:    "my-session-root",
			expected: "",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := guessSessionFromRootName(tt.input)
			if result != tt.expected {
				t.Errorf("guessSessionFromRootName(%q) = %q, want %q",
					tt.input, result, tt.expected)
			}
		})
	}
}

func TestGuessSessionFromWorkerName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "valid worker name",
			input:    "my-session-dllama-abc-workers",
			expected: "my-session",
		},
		{
			name:     "worker name without -workers suffix",
			input:    "my-session-dllama-abc",
			expected: "my-session",
		},
		{
			name:     "no dllama in name",
			input:    "my-session-workers",
			expected: "",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := guessSessionFromWorkerName(tt.input)
			if result != tt.expected {
				t.Errorf("guessSessionFromWorkerName(%q) = %q, want %q",
					tt.input, result, tt.expected)
			}
		})
	}
}

func TestEnsureOwnerReference(t *testing.T) {
	session := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-session",
			UID:  "session-uid-123",
		},
		TypeMeta: metav1.TypeMeta{
			APIVersion: "koldun.gorizond.io/v1",
			Kind:       "Session",
		},
	}

	tests := []struct {
		name            string
		existing        []metav1.OwnerReference
		expectedChanged bool
		expectedCount   int
	}{
		{
			name:            "no existing owner references",
			existing:        nil,
			expectedChanged: true,
			expectedCount:   1,
		},
		{
			name: "owner reference already exists with matching BlockOwnerDeletion",
			existing: []metav1.OwnerReference{
				{
					APIVersion:         "koldun.gorizond.io/v1",
					Kind:               "Session",
					Name:               "test-session",
					UID:                "session-uid-123",
					Controller:         boolPtr(true),
					BlockOwnerDeletion: boolPtr(true),
				},
			},
			expectedChanged: false,
			expectedCount:   1,
		},
		{
			name: "different owner reference exists",
			existing: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "other-resource",
					UID:        "other-uid",
				},
			},
			expectedChanged: true,
			expectedCount:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := &metav1.ObjectMeta{
				OwnerReferences: tt.existing,
			}

			changed := ensureOwnerReference(meta, session)

			if changed != tt.expectedChanged {
				t.Errorf("ensureOwnerReference() changed = %v, want %v",
					changed, tt.expectedChanged)
			}

			if len(meta.OwnerReferences) != tt.expectedCount {
				t.Errorf("owner references count = %d, want %d",
					len(meta.OwnerReferences), tt.expectedCount)
			}

			// Verify the session owner reference exists
			found := false
			for _, ref := range meta.OwnerReferences {
				if ref.UID == session.UID {
					found = true
					if ref.Name != session.Name {
						t.Errorf("owner reference name = %q, want %q",
							ref.Name, session.Name)
					}
					if ref.Kind != "Session" {
						t.Errorf("owner reference kind = %q, want Session", ref.Kind)
					}
					if ref.Controller == nil || !*ref.Controller {
						t.Error("owner reference should have controller=true")
					}
				}
			}
			if !found {
				t.Error("session owner reference not found in OwnerReferences")
			}
		})
	}
}

func boolPtr(b bool) *bool {
	return pointer.Bool(b)
}
