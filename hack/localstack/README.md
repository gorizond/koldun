# Local Integration Stack (docker-compose)

This stack provisions everything that the ingress tests expect without relying on ephemeral loopback sockets inside sandboxes. It starts:

- **k3s** (single-node Kubernetes API) — handy when you need a kubeconfig for envtest or manual smoke tests.
- **NATS JetStream** — configured with a single account (`koldun:koldun`) and on-disk JetStream storage under `nats-data`.
- **MinIO** — S3-compatible storage with the buckets `koldun-models` and `koldun-tokens` created automatically.

## Usage

```bash
# inside repo root
export COMPOSE_FILE=docker-compose.test.yml
export KOLDUN_STACK_PROFILE=tests   # optional marker for prompts/tools

# Bring the stack up
docker compose up -d

# Tail the logs, e.g. for NATS
docker compose logs -f nats

# Tear everything down when finished
docker compose down -v
```

Once `docker compose ps` reports all services as "healthy":

1. **Kubeconfig** is written to `hack/localstack/kubeconfig/kubeconfig`. Point `KUBECONFIG` or `KUBEBUILDER_ASSETS` helpers to it for controller/envtest work.
2. **NATS** is reachable at `nats://koldun:koldun@127.0.0.1:4222` with JetStream enabled. That satisfies the ingress tests that publish to `in.*` subjects.
3. **MinIO** listens on `http://127.0.0.1:9000` (`minio` / `minio123`). Buckets `koldun-models` and `koldun-tokens` already exist; feel free to create additional buckets for ad-hoc experiments via the console on port 9001.

## Integrating with `go test`

Set the same connection details the code expects before running coverage-heavy suites:

```bash
export KOLDUN_NATS_URL="nats://koldun:koldun@127.0.0.1:4222"
export KOLDUN_DISPATCHER_NATS_URL="$KOLDUN_NATS_URL"
export KOLDUN_MINIO_ENDPOINT="http://127.0.0.1:9000"
export KOLDUN_MINIO_ACCESS_KEY="minio"
export KOLDUN_MINIO_SECRET_KEY="minio123"
```

Then run the previously failing ingress tests:

```bash
GOCACHE=$PWD/.cache/go-build \
  go test ./pkg/servers/ingress -cover -count=1 -timeout=5m

# Dispatcher и retry-хелперы работают с тем же NATS:
GOCACHE=$PWD/.cache/go-build \
  go test ./pkg/servers/dispatcher -cover -count=1 -timeout=5m
```

## Notes
-	The stack intentionally leaves Traefik disabled inside k3s so that cluster networking stays quiet; add `--disable-network-policy=false` if you need more parity.
-	If you need TLS SANs for k3s, adjust `docker-compose.test.yml` and regenerate the kubeconfig under `hack/localstack/kubeconfig/`.
-	`minio-setup` exits once the buckets exist; re-run `docker compose up minio-setup` if you ever prune volumes.
-	JetStream persistence sits inside the named volume `nats-data`; delete it when you want a fresh KV/store.
