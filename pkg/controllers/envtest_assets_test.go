package controllers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScoreKubebuilderAssetPrefersHostOS(t *testing.T) {
	linuxScore := scoreKubebuilderAsset("/tmp/bin/envtest/k8s/1.32.0-linux-arm64", "linux", "arm64")
	darwinScore := scoreKubebuilderAsset("/tmp/bin/envtest/k8s/1.32.0-darwin-arm64", "linux", "arm64")
	require.Greater(t, linuxScore, darwinScore)
}

func TestScoreKubebuilderAssetPrefersMatchingArch(t *testing.T) {
	mismatched := scoreKubebuilderAsset("/tmp/bin/envtest/k8s/1.32.0-linux-amd64", "linux", "arm64")
	matching := scoreKubebuilderAsset("/tmp/bin/envtest/k8s/1.32.0-linux-arm64", "linux", "arm64")
	require.Greater(t, matching, mismatched)
}
