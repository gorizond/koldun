package controllers

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var (
	testEnv       *envtest.Environment
	testEnvConfig *rest.Config
)

// TestMain bootstraps the controller-runtime envtest environment shared across
// integration-style controller tests. The suite relies on the kubebuilder test
// binaries being available locally (set KUBEBUILDER_ASSETS when running
// locally). When the assets are not installed the tests are skipped instead of
// failing the entire package.
func TestMain(m *testing.M) {
	testEnv = &envtest.Environment{
		CRDInstallOptions: envtest.CRDInstallOptions{
			ErrorIfPathMissing: true,
		},
		CRDDirectoryPaths: []string{
			filepath.Join("charts", "koldun", "templates", "crd", "bases"),
		},
	}

	var err error
	testEnvConfig, err = testEnv.Start()
	if err != nil {
		if shouldSkipEnvtest(err) {
			fmt.Fprintf(os.Stderr, "envtest assets unavailable: %v\n", err)
			testEnv = nil
			testEnvConfig = nil
		} else {
			panic(err)
		}
	}

	code := m.Run()

	if testEnv != nil && testEnvConfig != nil {
		if stopErr := testEnv.Stop(); stopErr != nil {
			panic(stopErr)
		}
	}

	os.Exit(code)
}

func shouldSkipEnvtest(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "no such file or directory"),
		strings.Contains(msg, "failed to start the controlplane"),
		strings.Contains(msg, "unable to start control plane"):
		return true
	default:
		return false
	}
}
