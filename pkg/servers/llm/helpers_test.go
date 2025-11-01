package llm

import (
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
