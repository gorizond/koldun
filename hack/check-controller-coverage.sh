#!/usr/bin/env bash

# CI helper that verifies pkg/controllers coverage meets the minimum threshold.
# Used to enforce coverage regression gates in CI pipelines.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Minimum coverage percentage required (99% = 99.0)
MIN_COVERAGE="${MIN_COVERAGE:-99.0}"

log() {
  printf '[coverage-check] %s\n' "$*"
}

# Ensure KUBEBUILDER_ASSETS is set
if [[ -z "${KUBEBUILDER_ASSETS:-}" ]]; then
  if ! KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh 2>/dev/null)"; then
    log "ERROR: KUBEBUILDER_ASSETS not configured. Run 'make envtest-preflight' first."
    exit 1
  fi
  export KUBEBUILDER_ASSETS
fi

log "Using KUBEBUILDER_ASSETS=$KUBEBUILDER_ASSETS"

COVERAGE_FILE="$(mktemp)"
trap 'rm -f "$COVERAGE_FILE"' EXIT

log "Running go test with coverage profile..."
go test -coverprofile="$COVERAGE_FILE" ./pkg/controllers -timeout=10m

# Extract total coverage percentage
COVERAGE=$(go tool cover -func="$COVERAGE_FILE" | grep "^total:" | awk '{print $3}' | sed 's/%//')

log "Total coverage: ${COVERAGE}%"

# Compare with minimum (using bc for float comparison)
if command -v bc &>/dev/null; then
  PASS=$(echo "$COVERAGE >= $MIN_COVERAGE" | bc -l)
else
  # Fallback: truncate to integer comparison
  COVERAGE_INT="${COVERAGE%%.*}"
  MIN_INT="${MIN_COVERAGE%%.*}"
  if [[ "$COVERAGE_INT" -ge "$MIN_INT" ]]; then
    PASS=1
  else
    PASS=0
  fi
fi

if [[ "$PASS" -eq 1 ]]; then
  log "✓ Coverage ${COVERAGE}% meets minimum threshold ${MIN_COVERAGE}%"
  exit 0
else
  log "✗ Coverage ${COVERAGE}% is below minimum threshold ${MIN_COVERAGE}%"
  exit 1
fi
