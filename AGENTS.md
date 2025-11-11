# Repository Guidelines

## Project Structure & Module Organization
Core entrypoints live in `cmd/operator`, which selects operator, backend, dispatcher, or LLM sidecar modes via the `--mode` flag. Runtime servers reside in `pkg/servers` while controllers and reconcilers stay in `pkg/controllers`; custom resource types and generated clients sit under `pkg/apis/koldun.gorizond.io/v1`. Deployment assets are split between `charts/` for Helm, `k8s/` for raw manifests, and the root `Dockerfile` plus `skaffold.yaml` for container packaging. Tests follow the code they exercise, for example `pkg/controllers/memory_test.go`.

## Build, Test, and Development Commands
Run `go fmt ./... && gofmt -w` before committing to keep formatting consistent. Use `go build ./cmd/operator` to compile the unified binary or `go run ./cmd/operator --mode=operator` for a kubeconfig-backed smoke test. Execute `go test ./...` for unit coverage and add `-race` when debugging concurrency. Use `make controllers-smoke` (wrapper for `go test ./pkg/controllers -count=1 -timeout=5m`) as the lightweight envtest smoke for reconcilers. Container images are produced with `skaffold build`, publishing to `ghcr.io/gorizond/koldun`.

## Coding Style & Naming Conventions
Write Go 1.21+ idiomatic code: tabs for indentation, camelCase for locals, PascalCase for exported symbols. Shared helpers belong in `pkg/controllers/common.go`; resource-specific logic should remain in files such as `root.go` or `worker.go`. CLI flags mirror the existing pattern in `cmd/operator/main.go`, using kebab-case names prefixed by the target mode. Always run gofmt tooling and avoid introducing non-ASCII characters unless already present in a file.

## Testing Guidelines
Adopt table-driven tests with `_test.go` suffixes co-located beside implementations. Prioritize reconciliation branches, memory sizing helpers, and NATS interactions, mocking JetStream or Kubernetes clients to keep tests hermetic. Collect coverage with `go test ./...`, and extend with `-race` when chasing data races.

Envtest-powered controller tests (`pkg/controllers/dllama_reconcile_envtest_test.go`) require the kubebuilder toolchain. Install it once per machine and export `KUBEBUILDER_ASSETS`:

```bash
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
eval "$(setup-envtest use -p env --bin-dir ./bin/envtest 1.32.x!)"
```

The refreshed `envtest_suite_test.go` will skip gracefully (with guidance) when assets are missing, so CI logs stay quiet.

- Run `make envtest-preflight` (wrapper around `setup-envtest use`) whenever you bootstrap a new machine/runner; it verifies that `kube-apiserver` and `etcd` binaries exist under `./bin/envtest`.
- Export `KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"` (or whatever path `setup-envtest` prints) in your shell profile/CI job so `go test ./pkg/controllers` finds the binaries instantly. Direnv users can call the script inside `.envrc`.
- `./hack/print-kubebuilder-assets.sh` mirrors the auto-discovery logic from `envtest_suite_test.go` and prints the first directory that already contains the control-plane binaries. The Makefile and CI use it to avoid hardcoding platform-specific paths.
- Cache the entire `./bin/envtest` directory in CI to keep the controller smoke test under 5 seconds. Once the cache is reliable, set `KOLD_SKIP_ENVTEST_DOWNLOAD=1` so the suite fails fast instead of trying to auto-download assets during a run.

## Commit & Pull Request Guidelines
Prefer short, imperative commit subjects (for example `feat: add dispatcher autoscaling`), adding a scope when it clarifies impact. Pull requests should summarize behavioral changes, link issues, and list validation steps such as `go test ./...` or Helm smoke tests. Include relevant logs or screenshots for user-facing updates and confirm Helm charts, manifests, and Docker packaging stay in sync.

## Security & Configuration Tips
Keep credentials, NATS secrets, and hash keys in Kubernetes Secrets—never commit sensitive data. Revisit RBAC policies when shipping new controllers to ensure service accounts and JetStream permissions are scoped correctly. Update the Dockerfile and `skaffold.yaml` whenever build flags or binary names change so published images remain reproducible.
