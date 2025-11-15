# Koldun Operator 🧙‍♂️

## Overview
Koldun orchestrates distributed-llama inference topologies on Kubernetes. The single `cmd/operator` binary exposes controller, ingress backend, dispatcher, and LLM worker modes that coordinate model download, topology creation, NATS-based messaging, and conversation lifecycle management. Wrangler provides the reconciliation engine, and JetStream acts as the persistent queue for chat sessions, assignments, and registry metadata.

## Highlights
- Manages **Session → Dllama → Worker** hierarchies, wiring Deployments, StatefulSets, and Jobs for distributed-llama clusters.
- Streams Hugging Face models into S3/MinIO, converts artifacts (GGUF), and publishes sizing metadata for scheduling.
- Ingests chat traffic through an OpenAI-compatible backend that hashes API tokens, supervises dispatcher pools, and mirrors readiness back to clients.
- Automatically synchronises NATS connection URLs from `Ingress` resources to dependent `Dllama` objects to avoid configuration drift.

## Project Layout
- `cmd/operator/` — entrypoint wiring CLI flags to the desired mode.
- `pkg/apis/koldun.gorizond.io/v1/` — CRD type definitions and DeepCopy implementations.
- `pkg/controllers/` — Wrangler reconcilers plus helpers in `common.go`; tests (e.g. `memory_test.go`) sit beside implementations.
- `pkg/servers/{ingress,dispatcher,llm,operator}/` — HTTP servers and workers for runtime modes.
- `pkg/conversation`, `pkg/registry`, `pkg/tokens` — shared JetStream contracts, KV helpers, and token handling.
- `charts/` Helm chart, `k8s/` raw manifests, root `Dockerfile` and `skaffold.yaml` for image builds.

## Custom Resources
| Kind | Purpose | Notes |
| --- | --- | --- |
| `Session` | Supervises dispatcher Deployments and pools of generated `Dllama` resources per conversation hash. | Scales via `spec.sessionScaling` thresholds and stores queue prefixes/assignment buckets. |
| `Dllama` | Expands a distributed-llama topology (root + workers) for a specific model. | Ensures referenced `Model` exposes `status.outputPVCName`; inherits NATS URL from `Ingress` automatically. |
| `Model` | Downloads, converts, and sizes model artifacts. | Creates downloader/convert Jobs, S3 CSI PV/PVC, and publishes size metadata. |
| `Root` | Renders the distributed-llama root coordinator Deployment/Service. | Watches pods to update readiness. |
| `Worker` | Manages single-slot worker StatefulSets. | Tracks per-slot readiness for dispatcher accounting. |
| `Ingress` | Declares the public ingress/backend bundle. | Generates backend Deployment, Service, Kubernetes Ingress, and publishes NATS/registry configuration. |

## Binary Modes (`--mode`)
| Mode | What It Runs |
| --- | --- |
| `operator` (default) | Registers controllers, reconciles CRDs, publishes model/token registries, and synchronises JetStream conversation TTLs into `Session`/`Dllama` resources. |
| `ingress` (alias `backend`) | OpenAI-compatible HTTP edge; authenticates API tokens, maintains conversation KV records, and bridges requests to NATS. |
| `dispatcher` | Consumes backlog subjects, writes assignment KV entries, and fans work out to ready Dllama workers while tracking heartbeats. |
| `llm` | Sidecar-facing worker that streams completions between the dllama-api process and NATS `out.<hash>` subjects. |

## Conversation Flow
1. Backend validates an API token (mirrored from Secrets), computes `hash_koldun`, and stores a JSON manifest in the JetStream KV bucket (`backend-conversation-bucket`).
2. Operator watches the bucket and ensures matching `Session`/`Dllama` resources exist, labelling them with `koldun.gorizond.io/hash`.
3. Dispatcher reads backlog messages (`sessions.<hash>.requests`), assigns them to workers, and records progress in the assignments bucket.
4. LLM workers call the dllama-api sidecar, stream completion chunks to `out.<hash>`, and ping state subjects so the dispatcher can recycle slots.

## Getting Started
### Prerequisites
- Go 1.24+, Docker/Skaffold (for container builds), and access to a Kubernetes cluster (Kubernetes 1.30+) with JetStream-enabled NATS.
- Helm 3 if you plan to install via the included chart.

### Build & Run Locally
```bash
go fmt ./... && gofmt -w .
go build ./cmd/operator
# Run controllers against your kubeconfig
go run ./cmd/operator --mode=operator --kubeconfig ~/.kube/config
```

### Backend & Worker Smoke Tests
```bash
# Start the OpenAI-compatible ingress backend
KOLDUN_API_TOKEN=... \
go run ./cmd/operator --mode=ingress \
  --backend-namespace default \
  --backend-nats-url nats://user:pass@nats.default:4222

# Launch a standalone worker connected to an existing dispatcher
HASH_KOLDUN=... \
go run ./cmd/operator --mode=llm \
  --llm-request-subject "sessions.${HASH_KOLDUN}.dllama.0.in" \
  --llm-state-subject "sessions.${HASH_KOLDUN}.dllama.0.state"

# Launch a dispatcher pinned to one conversation backlog
HASH_KOLDUN=... \
go run ./cmd/operator --mode=dispatcher \
  --dispatcher-hash "${HASH_KOLDUN}" \
  --dispatcher-nats-url nats://koldun:k0ldun@nats.default:4222 \
  --dispatcher-backlog-subject "sessions.${HASH_KOLDUN}.requests" \
  --dispatcher-assignments-bucket koldun_assignments \
  --dispatcher-dllama-prefix "sessions.${HASH_KOLDUN}.dllama." \
  --dispatcher-state-prefix "sessions.${HASH_KOLDUN}.dllama." \
  --dispatcher-queue-group "dispatcher-${HASH_KOLDUN}" \
  --dispatcher-ack-wait 2m
```

### Dispatcher state subjects & metrics
- `--dispatcher-state-prefix` **must** match the subject prefix used by worker heartbeats and always end with `.`. When Sessions are generated via `Ingress` or Helm templates you can override this by setting `spec.backend.queue.stateStream`; if the stream already contains dots, the session controller copies it directly into the dispatcher args so manual Deployments stay in sync with CRD-driven ones.
- Set `--dispatcher-metrics-listen=:9090` (or another host:port) to expose `/metrics` and `/healthz` from dispatcher pods. The sample manifest in `k8s/dispatcher-deploy.yaml` enables the listener, opens port `9090`, and appends a ClusterIP Service plus PodMonitor example so Prometheus or readiness probes can scrape the endpoints immediately.
- Session CRDs expose `spec.dispatcherMetricsListen`, and `Ingress.spec.backend.dispatcherMetricsListen` along with `--backend-session-dispatcher-metrics-listen` keep the generated dispatcher Deployments in lock-step with manual pods so `/metrics` is always wired consistently. Helm users can also set `ingressDefaults.dispatcherMetricsListen` (or per-ingress overrides) to inject the flag without hand-editing every spec snippet.

Prometheus Operator setups can now reuse the PodMonitor example bundled with `k8s/dispatcher-deploy.yaml` (update the `release` label/namespace to match your stack). If you prefer Service-based scraping, the same manifest also ships a ClusterIP ready for ServiceMonitors; copy it per session dispatcher because the controller does not generate Services automatically to avoid per-session resource churn.

### Deploy to Kubernetes
- **Helm**: Edit `charts/koldun/values.yaml` (images, NATS, session scaling) then `helm install koldun charts/koldun`.
- **Raw manifests**: Apply the CRDs, controllers, and sample resources from the `k8s/` directory. Use `k8s/dispatcher-deploy.yaml` when you need a dedicated dispatcher Deployment; it demonstrates the required `--dispatcher-state-prefix` flag and the optional `--dispatcher-metrics-listen` endpoint, which now also propagates from `spec.dispatcherMetricsListen` on Sessions/Ingresses.
- Use `skaffold build` to publish the container image `ghcr.io/gorizond/koldun` before updating chart values.

## Example Custom Resources
### Model
Example `Model` that streams a Hugging Face repository into S3/MinIO, runs GGUF conversion, and exposes sizing metadata:

```yaml
apiVersion: koldun.gorizond.io/v1
kind: Model
metadata:
  name: mistral-convert
  namespace: default
spec:
  sourceUrl: https://huggingface.co/mistralai/Mistral-7B-v0.3
  localPath: s3://models/mistral-7b-v0-3
  objectStorage:
    endpoint: http://minio.default:32090
    bucketForSource: models
    bucketForConvert: models-converted
    secretRef:
      name: minio-creds
  download:
    image: python:3.10
    memory: 2Gi
    chunkMaxMiB: 256
    concurrency: 6
    huggingFaceTokenSecretRef:
      name: hf-token
  conversion:
    converterVersion: v0.16.2
    image: python:3.10
    memory: 8Gi
    convertWeights: q40
    outputPath: s3://models-converted/mistral-7b-v0-3
    toolsImage: alpine:3.18
  pipProxy: http://dragonfly.default:4001
```

- Downloader and converter Jobs mount the S3 buckets via CSI and publish size data to `status.conversionSizeBytes`.
- Set `download.huggingFaceTokenSecretRef` only for private repositories; the secret must provide `token`.
- Add the annotation `koldun.gorizond.io/force-size-rerun` to trigger a sizing rerun when artifacts change.

### Ingress
`Ingress` resources let the operator render the ingress/backend bundle and publish NATS details for dependent `Dllama` objects:

```yaml
apiVersion: koldun.gorizond.io/v1
kind: Ingress
metadata:
  name: public-backend
  namespace: default
spec:
  backend:
    image: ghcr.io/gorizond/koldun:latest
    rootImage: ghcr.io/gorizond/koldun:latest
    workerImage: ghcr.io/gorizond/koldun:latest
    dispatcherImage: ghcr.io/gorizond/koldun:latest
    replicaPower: 2
    nats:
      url: nats://koldun:k0ldun@nats.default:4222
      kvBucket: koldun_ttl
      modelsBucket: koldun_models
      tokensBucket: koldun_tokens
      modelPrefix: model/
      tokenPrefix: token/
    sessionScaling:
      minDllamas: 1
      maxDllamas: 4
      scaleUpBacklog: 2
      scaleDownIdleSeconds: 120
    conversationTTL: 10m
    responseTimeout: 2m
  service:
    port: 8082
  route:
    host: koldun.localtest.me
    path: /
    ingressClassName: traefik
```

- The controller produces the backend Deployment (`--mode=ingress`), Service, and Kubernetes Ingress automatically.
- Set `spec.backend.extraArgs` for advanced flags, or `spec.backend.hashSecret` to enable HMAC hashing.
- Use `spec.backend.queue.stateStream` to override dispatcher heartbeat subjects; when it contains dots the controller reuses it directly for `--dispatcher-state-prefix` so Deployments and Helm/k8s manifests stay in sync.
- Choosing `spec.service.type: LoadBalancer` or providing TLS annotations maps directly to the rendered Kubernetes Ingress.

## Development Workflow
- Follow `AGENTS.md` for contributor expectations, code layout, testing, and security conventions.
- Run `make help` to discover the canonical shortcuts (`test`, `controllers-smoke`, `compose-test`, `compose-update-baseline`) and reminders about compose coverage maintenance. Whenever the merged compose coverage exceeds the tracked value in `analytics/compose_coverage_baseline.json`, rerun `make compose-update-baseline`; the helper refreshes the JSON with the new percentage, timestamp, and commit hash so CI enforces the higher bar automatically.

## Local Integration Stack (docker-compose)
Run the NATS + MinIO + single-node k3s stack whenever you need a reproducible environment for ingress/dispatcher tests without depending on host networking quirks.

```bash
# from repo root
export COMPOSE_FILE=docker-compose.test.yml
docker compose up -d

# kubeconfig appears under hack/localstack/kubeconfig/kubeconfig
export KUBECONFIG=$PWD/hack/localstack/kubeconfig/kubeconfig

# sample env vars for go test ./pkg/servers/ingress
export KOLDUN_NATS_URL="nats://koldun:koldun@127.0.0.1:4222"
export KOLDUN_DISPATCHER_NATS_URL="$KOLDUN_NATS_URL"
export KOLDUN_MINIO_ENDPOINT="http://127.0.0.1:9000"
export KOLDUN_MINIO_ACCESS_KEY=minio
export KOLDUN_MINIO_SECRET_KEY=minio123
```

Bring the stack down with `docker compose down -v` when finished. See `hack/localstack/README.md` for full details on the services, health checks, and bucket/bootstrap logic.

### Automated Usage
- **Local**: `make compose-test` spins the stack up, waits for NATS/MinIO, and runs `go test` for both `pkg/servers/ingress` and `pkg/servers/dispatcher` against the same compose JetStream before tearing everything down. The target exports `KOLDUN_NATS_URL` (default `nats://koldun:koldun@127.0.0.1:4222`) and mirrors it into `KOLDUN_DISPATCHER_NATS_URL`, so dispatcher helpers automatically point at the compose stack. Each run emits `compose.coverprofile` (merged ingress+dispatcher coverage) and `artifacts/compose-logs.txt` with the full `docker compose logs` dump for easy upload/debugging.
- **CI**: `.github/workflows/compose-ingress.yaml` mirrors the same workflow on GitHub Actions so every PR touching ingress, dispatcher, or the compose stack runs the end-to-end tests with real JetStream/MinIO. The job shells out to `make compose-test`, runs `go tool cover -func compose.coverprofile`, publishes the total into the job summary (with a direct link to the uploaded `compose.coverage.txt`), highlights any coverage increase with a call-to-action, and fails if coverage drops below the baseline recorded in `analytics/compose_coverage_baseline.json` or if a PR raises coverage without updating that baseline. Artifacts `compose.coverprofile`, `compose.coverage.txt`, and `artifacts/compose-logs.txt` are uploaded on every run for offline inspection.
- **Coverage helpers**: after any local compose run, execute `go tool cover -func compose.coverprofile | tail -n 1` to inspect the “total” line and decide whether the baseline file should be updated. When you intentionally raise coverage, run `make compose-update-baseline` (wraps `hack/update-compose-coverage-baseline.sh $(COMPOSE_TEST_COVERPROFILE) $(COMPOSE_TEST_BASELINE)`) — the helper records the new total, UTC timestamp, and current commit hash in `analytics/compose_coverage_baseline.json` so CI enforces the higher target automatically.
- **Keep the stack running**: set `COMPOSE_TEST_KEEP_STACK=1 make compose-test` to skip the automatic teardown phase for interactive debugging (logs are still captured). Clean up manually with `make compose-test-down` once you are finished poking at the containers.
- **Dispatcher**: when running dispatcher tests manually, ensure the compose stack is up and run `export KOLDUN_DISPATCHER_NATS_URL=$KOLDUN_NATS_URL` (direnv users get this automatically via `.envrc`). Then execute `go test ./pkg/servers/dispatcher -cover -count=1` to validate backlog/retry flows without relying on loopback sockets.
- Shared helpers belong in `pkg/controllers/common.go`; resource-specific logic lives in dedicated files (`root.go`, `worker.go`, etc.).
- Prefer Go table-driven tests and mocks from `go.uber.org/mock` for JetStream/Kubernetes clients.
- Avoid committing secrets; store NATS credentials and hash secrets in Kubernetes Secrets labelled `koldun.gorizond.io/token`.

#### Troubleshooting
- **`ErrConnectionRefused` / `nats: no servers available` during dispatcher tests**: ensure `docker compose up` is running and both `KOLDUN_NATS_URL` and `KOLDUN_DISPATCHER_NATS_URL` point to the compose endpoint (`nats://koldun:koldun@127.0.0.1:4222`). `make compose-test` and `.envrc` already wire this up; for manual runs export the variables before invoking `go test`.
- Shared helpers belong in `pkg/controllers/common.go`; resource-specific logic lives in dedicated files (`root.go`, `worker.go`, etc.).
- Prefer Go table-driven tests and mocks from `go.uber.org/mock` for JetStream/Kubernetes clients.
- Avoid committing secrets; store NATS credentials and hash secrets in Kubernetes Secrets labelled `koldun.gorizond.io/token`.

### Envtest Integration Suite
Controllers rely on [`envtest`](https://book.kubebuilder.io/reference/envtest.html) binaries (`kube-apiserver`, `etcd`) when running the integration test in `pkg/controllers/dllama_reconcile_envtest_test.go`. Install the assets once and export `KUBEBUILDER_ASSETS` before running the suite:

```bash
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
# Downloads the Kubernetes stack compatible with controller-runtime v0.20.4 and prints the export lines
eval "$(setup-envtest use -p env --bin-dir ./bin/envtest 1.32.x!)"

# Persist for new shells / CI jobs (optional but recommended)
export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"
ls "$KUBEBUILDER_ASSETS"  # sanity-check kube-apiserver/etcd are present

# Verify the integration test; it will skip with a helpful message if assets are missing
go test ./pkg/controllers -run TestDllamaReconciliationCreatesRootAndWorker -count=1
```

If you want the same two-step sequence our onboarding docs and CI jobs follow, run:

```bash
make envtest-preflight
export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"
```

- #### Envtest quick start (new machine/runner)
  ```bash
  make envtest-preflight
  export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"
  /usr/bin/time -p make controllers-smoke
  ```
  Example log from the latest macOS arm64 run (cached envtest + module cache):

  ```text
  Running controller tests...
  Using KUBEBUILDER_ASSETS=/Users/negash/.agor/worktrees/gorizond/koldun/optimize/bin/envtest/k8s/1.32.0-darwin-arm64
  ok  	github.com/gorizond/koldun/pkg/controllers	39.729s
  ✓ All controller tests passed
  real 44.76
  user 8.96
  sys 4.39
  ```

  The helper prints the resolved `KUBEBUILDER_ASSETS` path, and wrapping the smoke test with `/usr/bin/time -p` gives a wall-clock baseline you can compare against future runs to spot missing caches or stalled envtest downloads. Prior to the Session 52 optimization, the suite took ~60 s because `TestConversationReconcilerMaintainsRecoveryTimeAcrossOutages` bootstrapped extra KV data and repeated cleanup; that section is now leaner (no pre-bootstrap, one reconnection loop per outage), hence the win. Keep the older ~60 s number in mind if you need to bisect regressions, but use the table below for current reference points on both macOS and Linux runners (cold = brand new checkout, cached = warmed envtest + module cache):

  | Runner | Envtest state | `/usr/bin/time -p make controllers-smoke` (real) | `go test ./pkg/controllers` duration | Notes |
  | --- | --- | --- | --- | --- |
  | macOS 15.1 (Apple Silicon) | cached (envtest + module cache restored) | 44.76 s real (user 8.96 s / sys 4.39 s) | 39.73 s | Same workstation used for Sessions 52–53 |
  | macOS 15.1 (Apple Silicon) | cold (fresh checkout, no caches) | 262.58 s real (user 120.46 s / sys 30.40 s) | 45.96 s | Includes first-run toolchain/module download; preceding `make envtest-preflight` adds 53.58 s |
| Linux (golang:1.22-bookworm container on the same host) | cached | 44.04 s real (user 6.04 s / sys 2.73 s) | 41.13 s | `GOTOOLCHAIN=go1.25`, `./hack/print-kubebuilder-assets.sh` auto-selects `bin/envtest/k8s/1.32.0-linux-arm64` based on `uname -s` |
| Linux (golang:1.22-bookworm container) | cold | 225.71 s real (user 117.14 s / sys 31.85 s) | 38.51 s | Includes downloading the Go toolchain + modules; envtest assets populated via `setup-envtest use` and auto-detected via `./hack/print-kubebuilder-assets.sh` |

  For troubleshooting slow or failing smoke tests, consult the **Envtest FAQ** in [`docs/ci-envtest.md`](docs/ci-envtest.md#envtest-faq); it now points back to this table so you can compare your runner against the cached/cold baselines.

- `make envtest-preflight` wraps the `setup-envtest use` invocation, validates that both `kube-apiserver` and `etcd` binaries exist, and reprints the `KUBEBUILDER_ASSETS` export line. Use it after toolchain upgrades or when bootstrapping CI runners.
- `.envrc` now exports `KUBEBUILDER_ASSETS=$(./hack/print-kubebuilder-assets.sh)`; run `direnv allow` (or copy the line into your shell profile) so controller tests discover the assets automatically.
- Capture the `KUBEBUILDER_ASSETS` path printed by `setup-envtest use` (typically `./bin/envtest/k8s/1.32.0-<os>-<arch>`) or run `./hack/print-kubebuilder-assets.sh` to auto-detect it for local shells and CI pipelines.
- Cache the `./bin/envtest` directory in CI runners to avoid downloading the binaries on every job; re-run `setup-envtest use` only when bumping controller-runtime.
- Once the cache exists, set `KOLD_SKIP_ENVTEST_DOWNLOAD=1` in CI (and optionally locally) so `ensureKubebuilderAssets()` fails fast if the binaries disappear instead of spending ~10 seconds trying to auto-download them.
- GitHub Actions runs these smoke tests in `.github/workflows/ci-build.yaml` via the `controllers-envtest` job. The job restores the `bin/envtest` cache, installs `setup-envtest`, executes `make envtest-preflight`, and blocks the Docker build job until `go test ./pkg/controllers -count=1 -timeout=10m` passes.
- Для любых CI раннеров (включая self-hosted) следуйте чек-листу в [`docs/ci-envtest.md`](docs/ci-envtest.md): восстановите кеш `bin/envtest`, выполните `make envtest-preflight`, экспортируйте `KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"`, а затем прогоните `make controllers-smoke`.
- Add the `export KUBEBUILDER_ASSETS=…` line to your shell profile (e.g. `.envrc`, `.zshrc`) so `go test` picks it up without re-running `setup-envtest`.
- The helper in `pkg/controllers/envtest_suite_test.go` auto-discovers `KUBEBUILDER_ASSETS`; when the binaries are absent the test suite now exits early with an explicit instruction instead of noisy control-plane failures.

### Controller Coverage Snapshot
- Generate a focused profile: `go test ./pkg/controllers -coverprofile=controllers.cover`
- Inspect watchers and sizing helpers: `go tool cover -func=controllers.cover | grep -E 'root.go|dllama.go|model_jobs.go'`
- Current reference (2025-11-02 19:30): controllers pkg coverage 77.9%; `worker.ensureStatefulSet` 88.0% (replica/memory planning paths covered), `worker.ensureStatus` 94.1% (ready + observedGeneration branches), root and worker watchers remain 100%, `persistModelAnnotation` error logging exercised via gomock, `ensureSizingJob` 87.9% (delete/apply errors mocked).
- Remove the temporary profile when finished (`rm controllers.cover`) to keep the workspace clean.

### Key Commands
| Purpose | Command |
| --- | --- |
| Format | `go fmt ./... && gofmt -w .` |
| Unit tests | `go test ./...` (append `-race` for data race checks) |
| Controllers smoke | `make controllers-smoke` (`go test ./pkg/controllers -count=1 -timeout=10m`) |
| Build binary | `go build ./cmd/operator` |
| Run operator | `go run ./cmd/operator --mode=operator` |
| Build/push image | `skaffold build` |

## Security & Configuration Notes
- Labels on Secrets (`koldun.gorizond.io/token=true`) trigger token mirroring into the JetStream registry bucket; the backend rejects disabled tokens (`stringData.disabled`).
- Set `backend-hash-secret` to enable HMAC-SHA256 conversation hashing; leave empty for plain SHA-256.
- The operator ensures S3 PV/PVC resources exist when `Model.spec.objectStorage` is configured; disable automatic bucket creation with `--operator-disable-bucket-ensure` when managing buckets manually.
- Update the Helm chart, Kubernetes manifests, and Dockerfile together when changing binary flags or images to avoid drift.

## Additional Resources
- Sample CRs: `k8s/examples/*.yaml` (models, dllama topologies, ingress definitions).
- Token tooling lives in `pkg/tokens`; registry helpers in `pkg/registry` show how JetStream buckets are structured.
- File an issue or PR with validation steps (`go test ./...`, Helm installation logs, kube events) to document behavioural changes.

## In Memoriam

I dedicate this repository to my grandfather, Negashev Vyacheslav Ivanovich
