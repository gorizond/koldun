package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gorizond/koldun/pkg/controllers"
	"github.com/rancher/wrangler/v3/pkg/signals"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog/v2"
)

func main() {
	var kubeconfig string
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig, falls back to in-cluster config")

	// Initialize klog flags
	klog.InitFlags(nil)
	flag.Parse()

	// Set klog to output to stderr by default
	klog.SetOutput(os.Stderr)

	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})

	ctx := signals.SetupSignalContext()

	cfg, err := buildConfig(kubeconfig)
	if err != nil {
		logrus.Fatalf("failed to build Kubernetes config: %v", err)
	}

	manager, err := controllers.NewManager(cfg)
	if err != nil {
		logrus.Fatalf("failed to create controller manager: %v", err)
	}

	if err := manager.Register(ctx); err != nil {
		logrus.Fatalf("failed to register controllers: %v", err)
	}

	logrus.Info("starting koldun operator")
	klog.Info("koldun operator is starting up")
	if err := manager.Start(ctx); err != nil {
		klog.Errorf("controller manager exited with error: %v", err)
		logrus.Fatalf("controller manager exited with error: %v", err)
	}
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err == nil {
			return cfg, nil
		}
		return nil, fmt.Errorf("failed to load kubeconfig %s: %w", kubeconfig, err)
	}

	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}

	if env := os.Getenv("KUBECONFIG"); env != "" {
		if cfg, err := clientcmd.BuildConfigFromFlags("", env); err == nil {
			return cfg, nil
		}
	}

	home := homedir.HomeDir()
	if home != "" {
		path := filepath.Join(home, ".kube", "config")
		if _, err := os.Stat(path); err == nil {
			return clientcmd.BuildConfigFromFlags("", path)
		}
	}

	return nil, fmt.Errorf("could not locate Kubernetes configuration")
}
