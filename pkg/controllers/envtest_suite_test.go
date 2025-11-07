package controllers

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
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

	assetsDir, locateErr := ensureKubebuilderAssets()
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

func ensureKubebuilderAssets() (string, error) {
	dir, err := locateKubebuilderAssets()
	if err == nil {
		return dir, nil
	}

	if downloadErr := autoDownloadKubebuilderAssets(); downloadErr != nil {
		return "", fmt.Errorf("%w; automatic setup-envtest download failed: %v", err, downloadErr)
	}

	return locateKubebuilderAssets()
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
	projectDir := projectRoot()

	expandPath := func(path string) []string {
		path = strings.TrimSpace(path)
		if path == "" {
			return nil
		}
		clean := filepath.Clean(path)
		if filepath.IsAbs(clean) || projectDir == "" {
			return []string{clean}
		}
		absolute := filepath.Join(projectDir, clean)
		if absolute == clean {
			return []string{clean}
		}
		return []string{clean, absolute}
	}

	seen := make(map[string]struct{})
	add := func(path string) {
		for _, candidate := range expandPath(path) {
			if candidate == "" {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
		}
	}

	if dir := os.Getenv("KUBEBUILDER_ASSETS"); dir != "" {
		add(dir)
	}

	var roots []string
	appendRoots := func(path string) {
		roots = append(roots, expandPath(path)...)
	}

	appendRoots(filepath.Join("bin", "envtest"))
	appendRoots(filepath.Join("bin", "kubebuilder"))
	appendRoots("/usr/local/kubebuilder")
	appendRoots("/usr/local/kubebuilder/bin")

	if home, err := os.UserHomeDir(); err == nil {
		appendRoots(filepath.Join(home, ".cache", "controller-runtime", "setup-envtest"))
		appendRoots(filepath.Join(home, ".cache", "kubebuilder-envtest"))
		appendRoots(filepath.Join(home, ".local", "share", "kubebuilder-envtest"))
		appendRoots(filepath.Join(home, "kubebuilder"))
		appendRoots(filepath.Join(home, "kubebuilder", "bin"))
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

func autoDownloadKubebuilderAssets() error {
	if skip := strings.TrimSpace(os.Getenv("KOLD_SKIP_ENVTEST_DOWNLOAD")); strings.EqualFold(skip, "1") || strings.EqualFold(skip, "true") {
		return fmt.Errorf("automatic download disabled by KOLD_SKIP_ENVTEST_DOWNLOAD=%s", skip)
	}

	setupPath, err := exec.LookPath("setup-envtest")
	if err != nil {
		return fmt.Errorf("setup-envtest binary not found in PATH: %w", err)
	}

	installDir := filepath.Join(projectRoot(), "bin", "envtest")
	if err := os.MkdirAll(installDir, 0o755); err != nil {
		return fmt.Errorf("create envtest install dir: %w", err)
	}

	fmt.Fprintf(os.Stderr, "attempting automatic envtest download via %s (install dir: %s)\n", setupPath, installDir)

	cmd := exec.Command(setupPath,
		"use",
		"--controller-runtime-version", controllerRuntimeVersion,
		"--install-dir", installDir,
	)
	cmd.Env = os.Environ()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("setup-envtest use failed: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}

	if assetsDir := parseKubebuilderAssetsExport(stdout.String()); assetsDir != "" {
		if err := os.Setenv("KUBEBUILDER_ASSETS", assetsDir); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to export KUBEBUILDER_ASSETS from setup-envtest output: %v\n", err)
		}
	}

	return nil
}

func parseKubebuilderAssetsExport(output string) string {
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "export KUBEBUILDER_ASSETS=") {
			continue
		}
		value := strings.TrimPrefix(line, "export KUBEBUILDER_ASSETS=")
		value = strings.Trim(value, `"'`)
		return value
	}
	return ""
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
