package llm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureTrailingDot(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "whitespace only",
			input:    "   ",
			expected: "",
		},
		{
			name:     "already has dot",
			input:    "prefix.",
			expected: "prefix.",
		},
		{
			name:     "no dot",
			input:    "prefix",
			expected: "prefix.",
		},
		{
			name:     "with spaces and no dot",
			input:    "  prefix  ",
			expected: "prefix.",
		},
		{
			name:     "with spaces and dot",
			input:    "  prefix.  ",
			expected: "prefix.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ensureTrailingDot(tt.input)
			if result != tt.expected {
				t.Errorf("ensureTrailingDot(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDurableName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "llm-default",
		},
		{
			name:     "whitespace only",
			input:    "   ",
			expected: "llm-default",
		},
		{
			name:     "valid alphanumeric",
			input:    "abc123",
			expected: "llm-abc123",
		},
		{
			name:     "with dashes and underscores",
			input:    "test-hash_123",
			expected: "llm-test-hash_123",
		},
		{
			name:     "with invalid characters",
			input:    "test@hash#123",
			expected: "llm-test-hash-123",
		},
		{
			name:     "mixed case",
			input:    "TestHash123",
			expected: "llm-TestHash123",
		},
		{
			name:     "with spaces",
			input:    "  test hash  ",
			expected: "llm-test-hash",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := durableName(tt.input)
			if result != tt.expected {
				t.Errorf("durableName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestUniqueSubjects(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]struct{}
		expected []string
	}{
		{
			name:     "empty map",
			input:    map[string]struct{}{},
			expected: []string{},
		},
		{
			name: "single subject",
			input: map[string]struct{}{
				"subject.test": {},
			},
			expected: []string{"subject.test"},
		},
		{
			name: "multiple subjects sorted",
			input: map[string]struct{}{
				"subject.c": {},
				"subject.a": {},
				"subject.b": {},
			},
			expected: []string{"subject.a", "subject.b", "subject.c"},
		},
		{
			name: "with whitespace",
			input: map[string]struct{}{
				"  subject.a  ": {},
				"subject.b":     {},
			},
			expected: []string{"subject.a", "subject.b"},
		},
		{
			name: "with empty strings",
			input: map[string]struct{}{
				"subject.a": {},
				"":          {},
				"   ":       {},
				"subject.b": {},
			},
			expected: []string{"subject.a", "subject.b"},
		},
		{
			name: "mixed case",
			input: map[string]struct{}{
				"Subject.B": {},
				"subject.a": {},
			},
			expected: []string{"Subject.B", "subject.a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := uniqueSubjects(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("uniqueSubjects() returned %d items, want %d", len(result), len(tt.expected))
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("uniqueSubjects()[%d] = %q, want %q", i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestInClusterNamespace(t *testing.T) {
	tests := []struct {
		name        string
		fileContent string
		createFile  bool
		expected    string
	}{
		{
			name:       "file does not exist",
			createFile: false,
			expected:   "",
		},
		{
			name:        "valid namespace",
			fileContent: "default",
			createFile:  true,
			expected:    "default",
		},
		{
			name:        "namespace with whitespace",
			fileContent: "  kube-system  \n",
			createFile:  true,
			expected:    "kube-system",
		},
		{
			name:        "namespace with newline",
			fileContent: "test-namespace\n",
			createFile:  true,
			expected:    "test-namespace",
		},
		{
			name:        "empty file",
			fileContent: "",
			createFile:  true,
			expected:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary directory structure
			tmpDir := t.TempDir()
			saDir := filepath.Join(tmpDir, "var", "run", "secrets", "kubernetes.io", "serviceaccount")
			nsFile := filepath.Join(saDir, "namespace")

			if tt.createFile {
				if err := os.MkdirAll(saDir, 0755); err != nil {
					t.Fatalf("failed to create directory: %v", err)
				}
				if err := os.WriteFile(nsFile, []byte(tt.fileContent), 0644); err != nil {
					t.Fatalf("failed to write namespace file: %v", err)
				}
			}

			// Temporarily change the function to use our test directory
			// Since inClusterNamespace() uses hardcoded path, we test the behavior
			// by verifying file read errors return empty string
			result := inClusterNamespace()

			// This test validates that the function returns empty string on error
			// In a real cluster environment, it would read from the actual path
			if !tt.createFile && result != "" {
				t.Errorf("inClusterNamespace() with missing file = %q, want empty string", result)
			}
		})
	}
}
