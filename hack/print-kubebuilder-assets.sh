#!/usr/bin/env bash

# Helper that prints the directory containing envtest control-plane binaries
# (kube-apiserver, etcd). It prefers the explicit KUBEBUILDER_ASSETS
# environment variable but can also discover assets downloaded via
# `setup-envtest use --bin-dir ./bin/envtest 1.32.x!`.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REQUIRED_BINS=("kube-apiserver" "etcd")

check_dir() {
	local dir="$1"
	if [[ -z "$dir" ]]; then
		return 1
	fi
	# Expand relative paths against the repository root.
	if [[ "$dir" != /* ]]; then
		dir="$ROOT_DIR/${dir#./}"
	fi
	if [[ ! -d "$dir" ]]; then
		return 1
	fi
	for bin in "${REQUIRED_BINS[@]}"; do
		if [[ ! -x "$dir/$bin" && ! -f "$dir/$bin" ]]; then
			return 1
		fi
	done
	( cd "$dir" >/dev/null 2>&1 && pwd )
}

if [[ -n "${KUBEBUILDER_ASSETS:-}" ]]; then
	if resolved="$(check_dir "$KUBEBUILDER_ASSETS")"; then
		printf '%s\n' "$resolved"
		exit 0
	fi
fi

shopt -s nullglob

declare -a candidates

# setup-envtest (controller-runtime >=0.20) stores assets under bin/envtest/k8s/<version>-<os>-<arch>
for dir in "$ROOT_DIR"/bin/envtest/k8s/*; do
	candidates+=("$dir")
done

# Older layout (kubebuilder/bin) plus common manual installs.
candidates+=(
	"$ROOT_DIR/bin/envtest/kubebuilder/bin"
	"$ROOT_DIR/bin/kubebuilder/bin"
	"/usr/local/kubebuilder/bin"
	"/usr/local/kubebuilder"
)

for dir in "${candidates[@]}"; do
	if resolved="$(check_dir "$dir")"; then
		printf '%s\n' "$resolved"
		exit 0
	fi
done

>&2 echo "kubebuilder assets not found; run 'make envtest-preflight' or rerun setup-envtest."
exit 1
