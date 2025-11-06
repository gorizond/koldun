package controllers

import "testing"

func TestNormalizeForceToken(t *testing.T) {
	got := normalizeForceToken("", true, "12345")
	if got != "annotation-rv-12345" {
		t.Fatalf("normalizeForceToken empty requested=true => %q, want %q", got, "annotation-rv-12345")
	}

	got = normalizeForceToken(" custom-token ", true, "12345")
	if got != "custom-token" {
		t.Fatalf("normalizeForceToken trims explicit token, got %q", got)
	}

	got = normalizeForceToken("", false, "67890")
	if got != "" {
		t.Fatalf("normalizeForceToken without request should keep empty, got %q", got)
	}
}

func TestForceTokenMatches(t *testing.T) {
	if !forceTokenMatches("token-123", "token-123", "v1") {
		t.Fatal("forceTokenMatches should match identical tokens")
	}
	if !forceTokenMatches("token-123", "token-123-v1", "v1") {
		t.Fatal("forceTokenMatches should treat suffixed processed token as match")
	}
	if forceTokenMatches("token-123", "token-123-v2", "v1") {
		t.Fatal("forceTokenMatches should not match different resourceVersion suffix")
	}
	if !forceTokenMatches("", "", "v1") {
		t.Fatal("forceTokenMatches should treat empty tokens as matching")
	}
	if forceTokenMatches("", "non-empty", "v1") {
		t.Fatal("forceTokenMatches should not match empty requested with non-empty processed")
	}
}
