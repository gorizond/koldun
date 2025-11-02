package kube

import (
	"os"
	"path/filepath"
	"testing"
)

const sampleKubeconfig = `
apiVersion: v1
kind: Config
clusters:
- name: local
  cluster:
    server: https://127.0.0.1:6443
    insecure-skip-tls-verify: true
contexts:
- name: local
  context:
    cluster: local
    user: local
current-context: local
users:
- name: local
  user:
    token: dummy
`

func writeKubeconfig(t *testing.T, dir string) string {
	t.Helper()

	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte(sampleKubeconfig), 0o644); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

func TestBuildConfigExplicitPath(t *testing.T) {
	dir := t.TempDir()
	path := writeKubeconfig(t, dir)

	cfg, err := BuildConfig(path)
	if err != nil {
		t.Fatalf("BuildConfig returned error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if got := cfg.Host; got != "https://127.0.0.1:6443" {
		t.Fatalf("unexpected host %q", got)
	}
}

func TestBuildConfigFromEnv(t *testing.T) {
	dir := t.TempDir()
	path := writeKubeconfig(t, dir)
	t.Setenv("KUBECONFIG", path)

	cfg, err := BuildConfig("")
	if err != nil {
		t.Fatalf("BuildConfig returned error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if got := cfg.Host; got != "https://127.0.0.1:6443" {
		t.Fatalf("unexpected host %q", got)
	}
}

func TestBuildConfigNoConfig(t *testing.T) {
	t.Setenv("KUBECONFIG", "")
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)
	t.Setenv("USERPROFILE", tempHome)

	cfg, err := BuildConfig("")
	if err == nil {
		t.Fatalf("expected error with no configuration, got config: %+v", cfg)
	}
}
