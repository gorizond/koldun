# Repository Guidelines

## Project Structure & Module Organization
Core entrypoints live in `cmd/operator`, which selects operator, backend, dispatcher, or LLM sidecar modes via the `--mode` flag. Runtime servers reside in `pkg/servers` while controllers and reconcilers stay in `pkg/controllers`; custom resource types and generated clients sit under `pkg/apis/koldun.gorizond.io/v1`. Deployment assets are split between `charts/` for Helm, `k8s/` for raw manifests, and the root `Dockerfile` plus `skaffold.yaml` for container packaging. Tests follow the code they exercise, for example `pkg/controllers/memory_test.go`.

## Build, Test, and Development Commands
Run `go fmt ./... && gofmt -w` before committing to keep formatting consistent. Use `go build ./cmd/operator` to compile the unified binary or `go run ./cmd/operator --mode=operator` for a kubeconfig-backed smoke test. Execute `go test ./...` for unit coverage and add `-race` when debugging concurrency. Container images are produced with `skaffold build`, publishing to `ghcr.io/gorizond/koldun`.

## Coding Style & Naming Conventions
Write Go 1.21+ idiomatic code: tabs for indentation, camelCase for locals, PascalCase for exported symbols. Shared helpers belong in `pkg/controllers/common.go`; resource-specific logic should remain in files such as `root.go` or `worker.go`. CLI flags mirror the existing pattern in `cmd/operator/main.go`, using kebab-case names prefixed by the target mode. Always run gofmt tooling and avoid introducing non-ASCII characters unless already present in a file.

## Testing Guidelines
Adopt table-driven tests with `_test.go` suffixes co-located beside implementations. Prioritize reconciliation branches, memory sizing helpers, and NATS interactions, mocking JetStream or Kubernetes clients to keep tests hermetic. Collect coverage with `go test ./...`, and extend with `-race` when chasing data races.

## Commit & Pull Request Guidelines
Prefer short, imperative commit subjects (for example `feat: add dispatcher autoscaling`), adding a scope when it clarifies impact. Pull requests should summarize behavioral changes, link issues, and list validation steps such as `go test ./...` or Helm smoke tests. Include relevant logs or screenshots for user-facing updates and confirm Helm charts, manifests, and Docker packaging stay in sync.

## Security & Configuration Tips
Keep credentials, NATS secrets, and hash keys in Kubernetes Secrets—never commit sensitive data. Revisit RBAC policies when shipping new controllers to ensure service accounts and JetStream permissions are scoped correctly. Update the Dockerfile and `skaffold.yaml` whenever build flags or binary names change so published images remain reproducible.
