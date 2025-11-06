package controllers

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const controllerRuntimeVersion = "0.20.4"

var (
	testEnv           *envtest.Environment
	testEnvConfig     *rest.Config
	envtestSkipReason string
)

// TestMain bootstraps the controller-runtime envtest environment shared across
// integration-style controller tests. The suite relies on the kubebuilder test
// binaries being available locally (set KUBEBUILDER_ASSETS when running
// locally). When the assets are not installed the tests are skipped instead of
// failing the entire package.
func TestMain(m *testing.M) {
	flag.Parse()

	assetsDir, locateErr := locateKubebuilderAssets()
	if locateErr != nil {
		envtestSkipReason = locateErr.Error()
		fmt.Fprintf(os.Stderr, "envtest disabled: %s\n", envtestSkipReason)
		code := m.Run()
		os.Exit(code)
	}

	if err := os.Setenv("KUBEBUILDER_ASSETS", assetsDir); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to export KUBEBUILDER_ASSETS: %v\n", err)
	}

	originalLevel := logrus.GetLevel()
	if !testing.Verbose() && originalLevel < logrus.WarnLevel {
		logrus.SetLevel(logrus.WarnLevel)
	}
	defer logrus.SetLevel(originalLevel)

	crdPath := filepath.Join(projectRoot(), "charts", "koldun", "templates", "crd", "bases")

	testEnv = &envtest.Environment{
		BinaryAssetsDirectory: assetsDir,
		CRDInstallOptions: envtest.CRDInstallOptions{
			ErrorIfPathMissing: true,
		},
		CRDDirectoryPaths: []string{crdPath},
	}

	var err error
	testEnvConfig, err = testEnv.Start()
	if err != nil {
		if shouldSkipEnvtest(err) {
			envtestSkipReason = fmt.Sprintf("envtest assets unavailable: %v", err)
			fmt.Fprintln(os.Stderr, envtestSkipReason)
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

func locateKubebuilderAssets() (string, error) {
	candidates := kubebuilderAssetCandidates()
	var attempted []string

	for _, dir := range candidates {
		path := strings.TrimSpace(dir)
		if path == "" {
			continue
		}

		info, err := os.Stat(path)
		if err != nil {
			attempted = append(attempted, fmt.Sprintf("%s (%v)", path, err))
			continue
		}
		if !info.IsDir() {
			continue
		}

		if err := validateKubebuilderAssets(path); err != nil {
			attempted = append(attempted, fmt.Sprintf("%s (%v)", path, err))
			continue
		}
		return path, nil
	}

	if len(attempted) == 0 {
		return "", fmt.Errorf("kubebuilder assets not found; install them with `setup-envtest use --controller-runtime-version %s --install-dir ./bin/envtest` and export KUBEBUILDER_ASSETS to the reported directory", controllerRuntimeVersion)
	}

	return "", fmt.Errorf("kubebuilder assets not found; install them with `setup-envtest use --controller-runtime-version %s --install-dir ./bin/envtest` and export KUBEBUILDER_ASSETS to the reported directory. Checked: %s", controllerRuntimeVersion, strings.Join(attempted, "; "))
}

func kubebuilderAssetCandidates() []string {
	seen := make(map[string]struct{})
	add := func(path string) {
		if path == "" {
			return
		}
		clean := filepath.Clean(path)
		if _, ok := seen[clean]; ok {
			return
		}
		seen[clean] = struct{}{}
	}

	var roots []string

	if dir := strings.TrimSpace(os.Getenv("KUBEBUILDER_ASSETS")); dir != "" {
		add(dir)
	}

	roots = append(roots,
		filepath.Join("bin", "envtest"),
		filepath.Join("bin", "kubebuilder"),
		"/usr/local/kubebuilder",
		"/usr/local/kubebuilder/bin",
	)

	if home, err := os.UserHomeDir(); err == nil {
		roots = append(roots,
			filepath.Join(home, ".cache", "controller-runtime", "setup-envtest"),
			filepath.Join(home, ".cache", "kubebuilder-envtest"),
			filepath.Join(home, ".local", "share", "kubebuilder-envtest"),
			filepath.Join(home, "kubebuilder"),
			filepath.Join(home, "kubebuilder", "bin"),
		)
	}

	for _, root := range roots {
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() {
			continue
		}
		add(root)
		for _, sub := range collectSubdirectories(root, 2) {
			add(sub)
		}
	}

	candidates := make([]string, 0, len(seen))
	for path := range seen {
		candidates = append(candidates, path)
	}
	sort.Strings(candidates)
	return candidates
}

func collectSubdirectories(root string, maxDepth int) []string {
	type item struct {
		path  string
		depth int
	}
	queue := []item{{path: root, depth: 0}}
	var dirs []string

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.depth != 0 {
			dirs = append(dirs, current.path)
		}
		if current.depth >= maxDepth {
			continue
		}

		entries, err := os.ReadDir(current.path)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				queue = append(queue, item{
					path:  filepath.Join(current.path, entry.Name()),
					depth: current.depth + 1,
				})
			}
		}
	}

	return dirs
}

func validateKubebuilderAssets(dir string) error {
	exe := ""
	if runtime.GOOS == "windows" {
		exe = ".exe"
	}

	required := []string{
		"kube-apiserver" + exe,
		"etcd" + exe,
	}

	var missing []string
	for _, bin := range required {
		if _, err := os.Stat(filepath.Join(dir, bin)); err != nil {
			missing = append(missing, bin)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing binaries: %s", strings.Join(missing, ", "))
	}

	return nil
}

func projectRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return dir
		}
		dir = parent
	}
}
