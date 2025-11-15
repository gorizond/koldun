# Repository Guidelines

## Project Structure & Module Organization
Core entrypoints live in `cmd/operator`, which selects operator, backend, dispatcher, or LLM sidecar modes via the `--mode` flag. Runtime servers reside in `pkg/servers` while controllers and reconcilers stay in `pkg/controllers`; custom resource types and generated clients sit under `pkg/apis/koldun.gorizond.io/v1`. Deployment assets are split between `charts/` for Helm, `k8s/` for raw manifests, and the root `Dockerfile` plus `skaffold.yaml` for container packaging. Tests follow the code they exercise, for example `pkg/controllers/memory_test.go`.

## Build, Test, and Development Commands
Run `go fmt ./... && gofmt -w` before committing to keep formatting consistent. Use `go build ./cmd/operator` to compile the unified binary or `go run ./cmd/operator --mode=operator` for a kubeconfig-backed smoke test. Execute `go test ./...` for unit coverage and add `-race` when debugging concurrency. Use `make controllers-smoke` (wrapper for `go test ./pkg/controllers -count=1 -timeout=10m`) as the lightweight envtest smoke for reconcilers. Container images are produced with `skaffold build`, publishing to `ghcr.io/gorizond/koldun`.

Run `make help` to discover the most common shortcuts (`test`, `controllers-smoke`, `compose-test`, and `compose-update-baseline`). Compose smoke tests (`make compose-test`) bring up the docker-compose stack, run ingress/dispatcher integration suites, and write `compose.coverprofile`. Whenever the total coverage exceeds the persisted baseline, run `make compose-update-baseline` to regenerate `analytics/compose_coverage_baseline.json`; the helper records the new percentage, timestamp, and commit hash so CI enforces the higher bar automatically.

## Coding Style & Naming Conventions
Write Go 1.21+ idiomatic code: tabs for indentation, camelCase for locals, PascalCase for exported symbols. Shared helpers belong in `pkg/controllers/common.go`; resource-specific logic should remain in files such as `root.go` or `worker.go`. CLI flags mirror the existing pattern in `cmd/operator/main.go`, using kebab-case names prefixed by the target mode. Always run gofmt tooling and avoid introducing non-ASCII characters unless already present in a file.

## Testing Guidelines
Adopt table-driven tests with `_test.go` suffixes co-located beside implementations. Prioritize reconciliation branches, memory sizing helpers, and NATS interactions, mocking JetStream or Kubernetes clients to keep tests hermetic. Collect coverage with `go test ./...`, and extend with `-race` when chasing data races.

Envtest-powered controller tests (`pkg/controllers/dllama_reconcile_envtest_test.go`) require the kubebuilder toolchain. Install it once per machine and export `KUBEBUILDER_ASSETS`:

```bash
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
eval "$(setup-envtest use -p env --bin-dir ./bin/envtest 1.32.x!)"
```

Or reuse the same two steps our onboarding, docs, and CI recommend:

```bash
make envtest-preflight
export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"
```

The refreshed `envtest_suite_test.go` will skip gracefully (with guidance) when assets are missing, so CI logs stay quiet.

- Run `make envtest-preflight` (wrapper around `setup-envtest use`) whenever you bootstrap a new machine/runner; it verifies that `kube-apiserver` and `etcd` binaries exist under `./bin/envtest`.
- Export `KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"` (or whatever path `setup-envtest` prints) in your shell profile/CI job so `go test ./pkg/controllers` finds the binaries instantly. Direnv users can call the script inside `.envrc`.
- `./hack/print-kubebuilder-assets.sh` mirrors the auto-discovery logic from `envtest_suite_test.go` and prints the first directory that already contains the control-plane binaries. The Makefile and CI use it to avoid hardcoding platform-specific paths.
- Cache the entire `./bin/envtest` directory in CI to keep the controller smoke test well under a minute. Session 51 (pre-optimization) clocked in at ~60s because of the JetStream recovery parity test; after trimming the bootstrap loops in Session 53 the same runner now finishes in ≈49s real. Sessions 56-57 added edge case tests raising coverage from 99.4% to **99.8%** (practical maximum). Once the cache is reliable, set `KOLD_SKIP_ENVTEST_DOWNLOAD=1` so the suite fails fast instead of trying to auto-download assets during a run.

### Envtest Quick Start

Run controller reconcilers against an in-memory Kubernetes API (envtest) to validate CRUD logic and integration flows without needing a live cluster.

**Prerequisites**:
- Go 1.21+ installed
- Make available
- Project dependencies fetched (`go mod download`)

**Step-by-step workflow**:

```bash
# Step 1: Install/verify envtest assets (one-time per machine)
make envtest-preflight

# Step 2: Export kubebuilder assets path
export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"

# Step 3: Run controller smoke tests with timing
/usr/bin/time -p make controllers-smoke
```

**Expected output**:
- All tests pass
- Timing baseline: ~49s real, 6.7s user, 5.6s sys on the cached macOS-arm64 runner; before the Session 52 optimization the same suite took ~60s, so keep both numbers handy when bisecting regressions
- Output ends with `✓ All controller tests passed` and the README now links to the Envtest FAQ for troubleshooting slow runs

**Troubleshooting**:
- **"unable to find binaries"**: `KUBEBUILDER_ASSETS` not set. Run Step 2 above or add to shell profile/direnv.
- **"kube-apiserver binary missing"**: Assets download incomplete. Re-run `make envtest-preflight`.
- **Tests hang or timeout**: Check for NATS connection issues, improper cleanup in test code, or deadlock from `-coverprofile` + `t.Parallel` (see `controllers-smoke` Makefile target).

### Controllers Smoke Test Coverage

**Purpose**: Fast validation of all controller reconcilers using envtest (no external dependencies except embedded NATS).

**Current test files**:
- `pkg/controllers/conversation_reconcile_envtest_test.go` — Conversation → Session CRUD via NATS KV
- `pkg/controllers/session_reconcile_envtest_test.go` — Session → Dllama/Dispatcher resource creation
- `pkg/controllers/dllama_reconcile_envtest_test.go` — Dllama resource reconciliation
- `pkg/controllers/envtest_suite_test.go` — Shared envtest setup and asset detection

**Key test scenarios** (Session 47 additions):

1. **KV Bucket Recovery** (`TestConversationReconcilerCreatesSessionFromKV`):
   - Creates Session resource from NATS KV entry
   - Deletes KV entry, verifies Session cleanup
   - **Simulates JetStream KV bucket deletion during runtime**
   - Verifies reconciler reconnects and recreates bucket
   - Confirms new KV entries properly create/delete Session resources after recovery
   - Tests NATS server restart + bucket recreation flow
   - Location: `pkg/controllers/conversation_reconcile_envtest_test.go:19`

2. **Graceful Shutdown** (`TestStartConversationReconcilerStopsWhenContextCancelled`):
   - Verifies reconciler drains NATS connections on context cancellation
   - Location: `pkg/controllers/conversation_reconcile_envtest_test.go:244`
3. **Orphan Cleanup After Bucket Loss** (`TestConversationReconcilerCleansOrphanedSessionsAfterBucketLoss`):
   - Leaves Session objects alive while deleting the entire KV bucket, ensures the reconciler reconnects, drops orphaned Sessions, and continues to accept new records
   - Location: `pkg/controllers/conversation_reconcile_envtest_test.go:205`

**When to run**:
- After controller logic changes (reconcile loops, error handling, NATS integration)
- Before committing reconciler updates
- As CI validation step (cached assets → <5s runtime)

## Commit & Pull Request Guidelines
Prefer short, imperative commit subjects (for example `feat: add dispatcher autoscaling`), adding a scope when it clarifies impact. Pull requests should summarize behavioral changes, link issues, and list validation steps such as `go test ./...` or Helm smoke tests. Include relevant logs or screenshots for user-facing updates and confirm Helm charts, manifests, and Docker packaging stay in sync.

## Security & Configuration Tips
Keep credentials, NATS secrets, and hash keys in Kubernetes Secrets—never commit sensitive data. Revisit RBAC policies when shipping new controllers to ensure service accounts and JetStream permissions are scoped correctly. Update the Dockerfile and `skaffold.yaml` whenever build flags or binary names change so published images remain reproducible.

- Dispatcher mode now enforces `--dispatcher-state-prefix` (non-empty and ending in `.`). Update any Helm values, raw manifests, or hand-run commands that start `cmd/operator --mode=dispatcher` to include the flag so the server can subscribe to worker state subjects.
- Sessions generated via CRDs inherit the value from `spec.queue.dllamaSubjectPrefix` unless you set `spec.queue.stateStream`; providing a dotted stream name (for example `sessions.hash.state`) forces the controller to emit the same string for `--dispatcher-state-prefix`, keeping manual Deployments aligned with declarative resources.
- Enable Prometheus/health scraping for dispatcher pods by supplying `--dispatcher-metrics-listen` (e.g. `:9090`) in manifests or CLI invocations. `spec.dispatcherMetricsListen` on Sessions (surfaced via `Ingress.spec.backend.dispatcherMetricsListen` or `--backend-session-dispatcher-metrics-listen`) keeps generated Deployments aligned, Helm charts can now inject the value via `ingressDefaults.dispatcherMetricsListen`, and `k8s/dispatcher-deploy.yaml` ships a ready-made Service plus PodMonitor template beside the Deployment so scraping works out of the box.
