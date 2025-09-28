package kube

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// BuildConfig resolves a Kubernetes REST configuration following the
// conventional kubeconfig lookup chain used by the operator.
func BuildConfig(explicitPath string) (*rest.Config, error) {
	if explicitPath != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", explicitPath)
		if err == nil {
			return cfg, nil
		}
		return nil, fmt.Errorf("failed to load kubeconfig %s: %w", explicitPath, err)
	}

	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}

	if env := os.Getenv("KUBECONFIG"); env != "" {
		if cfg, err := clientcmd.BuildConfigFromFlags("", env); err == nil {
			return cfg, nil
		}
	}

	if home := homedir.HomeDir(); home != "" {
		path := filepath.Join(home, ".kube", "config")
		if _, err := os.Stat(path); err == nil {
			return clientcmd.BuildConfigFromFlags("", path)
		}
	}

	return nil, fmt.Errorf("could not locate Kubernetes configuration")
}
