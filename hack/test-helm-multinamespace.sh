#!/usr/bin/env bash
# Renders the chart with an Ingress CR outside the release namespace and checks
# that the dedicated dllama service account/RBAC stay in the release namespace.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT

helm template koldun "$ROOT_DIR/charts/koldun" \
  --namespace koldun-system \
  -f "$ROOT_DIR/charts/koldun/tests/multinamespace-values.yaml" >"$OUT"

grep -q "namespace: other-ns" "$OUT" || {
  echo "multi-namespace Ingress was not rendered" >&2
  exit 1
}

grep -A 2 "name: koldun-dllama" "$OUT" | grep -q "namespace: koldun-system" || {
  echo "dedicated dllama service account was not rendered in the release namespace" >&2
  exit 1
}

grep -q "name: POD_NAMESPACE" "$OUT" || {
  echo "deployment is missing POD_NAMESPACE" >&2
  exit 1
}

echo "multi-namespace helm render OK"
