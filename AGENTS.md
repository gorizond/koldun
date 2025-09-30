# Repository Guidelines

## Project Structure & Module Organization
The operator entrypoint lives in `cmd/operator`, wiring Wrangler controllers and CLI modes. Custom resource types sit in `pkg/apis/koldun.gorizond.io/v1`, while reconcile logic and helpers are under `pkg/controllers` (see `common.go`, `manager.go`, and resource-specific files). Helm packaging for cluster installs is in `charts/`, and `k8s/` holds raw manifests for quick smoke deployment. Container build artifacts are defined in the root `Dockerfile` and `skaffold.yaml`; keep them aligned whenever dependencies or flags change. A prebuilt `operator` binary may exist for fast prototyping—regenerate it from source before publishing.

## Build, Test, and Development Commands
Use Go 1.21+.
```bash
go fmt ./...
go build ./cmd/operator
go run ./cmd/operator --mode=operator
go test ./...
skaffold build
```
`go fmt` enforces canonical formatting, `go build` produces the controller binary, `go run` executes it against your current kubeconfig, `go test` runs unit tests (currently focused on controller behaviour), and `skaffold build` rebuilds the container image `ghcr.io/gorizond/koldun` using the root Dockerfile.

## Coding Style & Naming Conventions
Stick to idiomatic Go: tab indentation, PascalCase for exported API types, camelCase for locals, and snake_case file names. Group new controllers by resource (`dllama.go`, `model.go`, etc.) and reuse `common.go` for shared helpers. Always run `go fmt` and `goimports`, prefer structured logging via the Wrangler logger, and keep package boundaries clean (`pkg/apis` for types, `pkg/controllers` for logic).

## Testing Guidelines
Locate tests beside implementation under `pkg/`, naming them `*_test.go` (e.g. `pkg/controllers/dllama_test.go`). Use Go’s `testing` package with table-driven cases to cover reconciliation branches, NATS URL propagation, and error paths. Run `go test ./...` before submitting changes; add `-race` when investigating concurrency-sensitive bugs. Document any required Kubernetes fixtures or fake clients in the test file header.

## Commit & Pull Request Guidelines
Write imperative, present-tense commit subjects (~72 chars). Prefix with a scope when it clarifies intent (`chore(ci): …`, `feat:`), mirroring the existing history. For pull requests, link issues, summarize operator or chart impact, list manual verification steps (`go test ./...`, cluster smoke install), and attach logs or screenshots when behaviour is user-visible. Request reviews from controller maintainers and ensure Helm values or manifests are updated in tandem with code changes.
