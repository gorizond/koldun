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
