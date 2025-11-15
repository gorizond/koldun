#!/usr/bin/env bash

# CI helper that warms envtest assets and runs the controllers smoke suite.
# It encapsulates the checklist from docs/ci-envtest.md so GitHub Actions and
# other runners stay consistent with local development.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

log() {
  printf '[ci-envtest] %s\n' "$*"
}

if [[ "${SKIP_SETUP_ENVTEST_INSTALL:-0}" != "1" ]]; then
  log "Installing setup-envtest tool"
  go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
else
  log "Skipping setup-envtest installation (SKIP_SETUP_ENVTEST_INSTALL=1)"
fi

log "Running make envtest-preflight"
make envtest-preflight

export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"
log "Using KUBEBUILDER_ASSETS=$KUBEBUILDER_ASSETS"

export KOLD_SKIP_ENVTEST_DOWNLOAD="${KOLD_SKIP_ENVTEST_DOWNLOAD:-1}"

# Check if coverage gate is enabled (default: enabled in CI)
if [[ "${SKIP_COVERAGE_CHECK:-0}" != "1" ]]; then
  log "Running controllers coverage check (make controllers-coverage-check)"
  make controllers-coverage-check
else
  log "Running controllers smoke (make controllers-smoke)"
  make controllers-smoke
fi
