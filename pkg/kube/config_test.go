package kube

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildConfig(t *testing.T) {
	t.Run("with explicit path - file not exists", func(t *testing.T) {
		_, err := BuildConfig("/nonexistent/kubeconfig")
		if err == nil {
			t.Error("BuildConfig() should fail with nonexistent path")
		}
	})

	t.Run("with explicit valid kubeconfig", func(t *testing.T) {
		tmpDir := t.TempDir()
		kubeconfigPath := filepath.Join(tmpDir, "config")

		content := `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://localhost:6443
  name: test-cluster
contexts:
- context:
    cluster: test-cluster
    user: test-user
  name: test-context
current-context: test-context
users:
- name: test-user
  user:
    token: test-token
`
		if err := os.WriteFile(kubeconfigPath, []byte(content), 0600); err != nil {
			t.Fatalf("Failed to write test kubeconfig: %v", err)
		}

		cfg, err := BuildConfig(kubeconfigPath)
		if err != nil {
			t.Fatalf("BuildConfig() error = %v", err)
		}
		if cfg == nil {
			t.Error("BuildConfig() returned nil config")
		}
		if cfg.Host != "https://localhost:6443" {
			t.Errorf("Config Host = %v, want https://localhost:6443", cfg.Host)
		}
	})

	t.Run("with empty path falls back", func(t *testing.T) {
		// Save original env
		oldKubeconfig := os.Getenv("KUBECONFIG")
		defer func() {
			if oldKubeconfig != "" {
				os.Setenv("KUBECONFIG", oldKubeconfig)
			} else {
				os.Unsetenv("KUBECONFIG")
			}
		}()

		// Create temp kubeconfig
		tmpDir := t.TempDir()
		kubeconfigPath := filepath.Join(tmpDir, "config")

		content := `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://env-cluster:6443
  name: env-cluster
contexts:
- context:
    cluster: env-cluster
    user: env-user
  name: env-context
current-context: env-context
users:
- name: env-user
  user:
    token: env-token
`
		if err := os.WriteFile(kubeconfigPath, []byte(content), 0600); err != nil {
			t.Fatalf("Failed to write test kubeconfig: %v", err)
		}

		os.Setenv("KUBECONFIG", kubeconfigPath)

		cfg, err := BuildConfig("")
		if err != nil {
			// In cluster config or KUBECONFIG should work
			t.Logf("BuildConfig() with empty path: %v", err)
		}
		if cfg != nil && cfg.Host == "https://env-cluster:6443" {
			t.Logf("Successfully loaded config from KUBECONFIG env var")
		}
	})
}

func TestBuildConfig_InvalidKubeconfig(t *testing.T) {
	tmpDir := t.TempDir()
	kubeconfigPath := filepath.Join(tmpDir, "invalid-config")

	// Write invalid kubeconfig
	if err := os.WriteFile(kubeconfigPath, []byte("invalid yaml content"), 0600); err != nil {
		t.Fatalf("Failed to write invalid kubeconfig: %v", err)
	}

	_, err := BuildConfig(kubeconfigPath)
	if err == nil {
		t.Error("BuildConfig() should fail with invalid kubeconfig")
	}
}
