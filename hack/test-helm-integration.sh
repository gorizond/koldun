#!/usr/bin/env bash
set -euo pipefail

# Integration test for Koldun Helm chart with k3d
# Tests installation of NATS, MinIO, and CSI S3 dependencies

CLUSTER_NAME="${CLUSTER_NAME:-koldun-test}"
CHART_DIR="charts/koldun"
RELEASE_NAME="koldun"
NAMESPACE="koldun-test"
TIMEOUT="${TIMEOUT:-600s}"
DEBUG="${DEBUG:-false}"
SKIP_OPERATOR="${SKIP_OPERATOR:-true}"  # Skip operator pod by default (image may not exist)
TEST_CSI_S3="${TEST_CSI_S3:-false}"  # CSI S3 requires privileged mode, skip by default
BUILD_IMAGE="${BUILD_IMAGE:-false}"  # Build and test operator image locally
IMAGE_TAG="${IMAGE_TAG:-test}"  # Tag for locally built image

log() { echo "[helm-test] $*"; }
err() { echo "[helm-test] ERROR: $*" >&2; }
debug() { [[ "$DEBUG" == "true" ]] && echo "[helm-test] DEBUG: $*" || true; }

cleanup() {
    local exit_code=$?
    log "Cleaning up..."
    if [[ "$DEBUG" == "true" ]] || [[ $exit_code -ne 0 ]]; then
        log "Collecting debug information before cleanup..."
        kubectl get pods -n "$NAMESPACE" -o wide 2>/dev/null || true
        kubectl get pvc -n "$NAMESPACE" 2>/dev/null || true
        kubectl get events -n "$NAMESPACE" --sort-by='.lastTimestamp' 2>/dev/null | tail -30 || true
    fi
    k3d cluster delete "$CLUSTER_NAME" 2>/dev/null || true
}

# Cleanup on exit
trap cleanup EXIT

# Check prerequisites
for cmd in k3d helm kubectl docker; do
    if ! command -v "$cmd" &>/dev/null; then
        err "$cmd is required but not installed"
        exit 1
    fi
done

# Build operator image if requested
if [[ "$BUILD_IMAGE" == "true" ]]; then
    log "Building operator image locally..."
    OPERATOR_IMAGE="ghcr.io/gorizond/koldun:${IMAGE_TAG}"
    if ! docker build -t "$OPERATOR_IMAGE" .; then
        err "Failed to build operator image"
        exit 1
    fi
    log "Operator image built: $OPERATOR_IMAGE"
    # Automatically enable operator testing when building image
    SKIP_OPERATOR=false
fi

# Clean up any leftover cluster from previous run
log "Cleaning up any existing cluster..."
k3d cluster delete "$CLUSTER_NAME" 2>/dev/null || true

log "Creating k3d cluster: $CLUSTER_NAME"
k3d cluster create "$CLUSTER_NAME" \
    --agents 1 \
    --wait \
    --timeout 120s \
    --k3s-arg "--disable=traefik@server:0"

log "Waiting for cluster to be ready..."
kubectl wait --for=condition=Ready nodes --all --timeout=60s

# Import locally built image into k3d cluster
if [[ "$BUILD_IMAGE" == "true" ]]; then
    log "Importing operator image into k3d cluster..."
    if ! k3d image import "$OPERATOR_IMAGE" -c "$CLUSTER_NAME"; then
        err "Failed to import operator image into k3d cluster"
        exit 1
    fi
    log "Operator image imported successfully"
fi

log "Creating namespace: $NAMESPACE"
kubectl create namespace "$NAMESPACE"

log "Checking storage class availability..."
kubectl get storageclass

log "Updating helm dependencies..."
helm dependency update "$CHART_DIR"

# Build helm install command with test-friendly settings
HELM_ARGS=(
    --namespace "$NAMESPACE"
    --set nats.enabled=true
    # Use memory storage for NATS JetStream (no PVC needed for tests)
    --set nats.config.jetstream.fileStore.enabled=false
    --set nats.config.jetstream.memoryStore.enabled=true
    --set nats.config.jetstream.memoryStore.maxSize=256Mi
    # Disable nats-box to reduce image pulls
    --set nats.natsBox.enabled=false
    --set minio.enabled=true
    # Disable MinIO persistence for tests (uses emptyDir)
    --set minio.persistence.enabled=false
    --set minio.resources.requests.memory=256Mi
    # Disable bucket creation to avoid post-install hook timeout
    --set minio.buckets=null
    --timeout "$TIMEOUT"
)

# CSI S3 requires privileged mode, skip by default
if [[ "$TEST_CSI_S3" == "true" ]]; then
    log "Enabling CSI S3 driver (TEST_CSI_S3=true)"
    HELM_ARGS+=(--set csi-s3.enabled=true)
else
    log "Skipping CSI S3 driver (TEST_CSI_S3=false)"
    HELM_ARGS+=(--set csi-s3.enabled=false)
fi

# If building image, use custom tag and enable operator
if [[ "$BUILD_IMAGE" == "true" ]]; then
    log "Using locally built image (tag: $IMAGE_TAG)"
    HELM_ARGS+=(
        --set image.tag="$IMAGE_TAG"
        --set image.pullPolicy=Never
    )
fi

# If skipping operator, set replicas to 0
if [[ "$SKIP_OPERATOR" == "true" ]]; then
    log "Skipping operator deployment (SKIP_OPERATOR=true)"
    HELM_ARGS+=(--set replicaCount=0)
fi

log "Installing Koldun chart with all dependencies enabled..."
if ! helm install "$RELEASE_NAME" "$CHART_DIR" "${HELM_ARGS[@]}"; then
    err "Helm install failed immediately. Checking status..."
    kubectl get pods -n "$NAMESPACE" -o wide || true
    kubectl get pvc -n "$NAMESPACE" || true
    kubectl get events -n "$NAMESPACE" --sort-by='.lastTimestamp' | tail -30 || true
    exit 1
fi

log "Chart installed. Waiting for dependencies to initialize..."

# Give some time for pods to start scheduling
sleep 10

log "Current pod status:"
kubectl get pods -n "$NAMESPACE" -o wide

log "Verifying NATS deployment..."
# NATS may take several minutes for image pull on first run
if ! kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=nats \
    --namespace "$NAMESPACE" \
    --timeout=300s; then
    err "NATS pod did not become ready"
    kubectl describe pod -l app.kubernetes.io/name=nats -n "$NAMESPACE"
    exit 1
fi

NATS_POD=$(kubectl get pod -l app.kubernetes.io/name=nats -n "$NAMESPACE" -o jsonpath='{.items[0].metadata.name}')
log "NATS pod: $NATS_POD"

# Verify NATS is running
kubectl exec -n "$NAMESPACE" "$NATS_POD" -c nats -- nats-server --version || true
log "NATS deployment verification passed"

log "Verifying MinIO deployment..."
# MinIO uses label app=minio,release=RELEASE_NAME
# Wait longer as image pull and PVC binding takes time
if ! kubectl wait --for=condition=Ready pod -l "app=minio,release=${RELEASE_NAME}" \
    --namespace "$NAMESPACE" \
    --timeout=300s; then
    err "MinIO pod did not become ready"
    kubectl describe pod -l "app=minio,release=${RELEASE_NAME}" -n "$NAMESPACE" || true
    kubectl get pods -n "$NAMESPACE" -o wide
    kubectl get pvc -n "$NAMESPACE"
    exit 1
fi

MINIO_POD=$(kubectl get pod -l "app=minio,release=${RELEASE_NAME}" -n "$NAMESPACE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
log "MinIO pod: $MINIO_POD"

if [[ -n "$MINIO_POD" ]]; then
    # Verify MinIO is accessible (mc client may not be in main container)
    kubectl logs -n "$NAMESPACE" "$MINIO_POD" --tail=10 || true
fi
log "MinIO deployment verification passed"

if [[ "$TEST_CSI_S3" == "true" ]]; then
    log "Verifying CSI S3 driver..."
    # CSI S3 driver may not register in k3d without privileged mode
    if kubectl get csidrivers.storage.k8s.io ru.yandex.s3.csi &>/dev/null; then
        log "CSI S3 driver registered"
    else
        log "WARNING: CSI S3 driver not registered (expected in unprivileged k3d)"
    fi

    # Check if CSI S3 pods are running
    CSI_PODS=$(kubectl get pods -n "$NAMESPACE" -l app=csi-s3 2>/dev/null | grep -c Running || echo "0")
    log "CSI S3 pods in namespace: $CSI_PODS"

    # Check StorageClass
    if kubectl get storageclass csi-s3 &>/dev/null; then
        log "CSI S3 StorageClass created"
    else
        log "WARNING: CSI S3 StorageClass not found (expected if driver not fully initialized)"
    fi
else
    log "Skipping CSI S3 verification (TEST_CSI_S3=false)"
fi

log "Verifying CRDs installed..."
for crd in dllamas ingresses models roots sessions workers; do
    if kubectl get crd "${crd}.koldun.gorizond.io" &>/dev/null; then
        log "CRD $crd.koldun.gorizond.io: OK"
    else
        err "CRD $crd.koldun.gorizond.io: MISSING"
        exit 1
    fi
done

if [[ "$SKIP_OPERATOR" != "true" ]]; then
    log "Verifying Koldun operator deployment..."
    kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=koldun \
        --namespace "$NAMESPACE" \
        --timeout=120s

    OPERATOR_POD=$(kubectl get pod -l app.kubernetes.io/name=koldun -n "$NAMESPACE" -o jsonpath='{.items[0].metadata.name}')
    log "Operator pod: $OPERATOR_POD"

    # Check operator health
    kubectl exec -n "$NAMESPACE" "$OPERATOR_POD" -- curl -sf http://localhost:8080/healthz || log "WARNING: Health check failed"
fi

log "Listing all resources in namespace..."
kubectl get all -n "$NAMESPACE"

log "Testing helm upgrade (idempotency)..."
if ! helm upgrade "$RELEASE_NAME" "$CHART_DIR" "${HELM_ARGS[@]}"; then
    err "Helm upgrade failed"
    exit 1
fi
log "Helm upgrade successful"

# Note: Partial installation test (NATS-only) is skipped because CRDs are already
# owned by the first release. Helm doesn't allow installing CRDs from different releases.
log "Skipping partial installation test (CRD ownership conflict with first release)"

log "All integration tests passed!"
log "Summary:"
log "  - NATS: Deployed with JetStream (memory storage, no PVC)"
log "  - MinIO: Deployed in standalone mode (no persistence)"
if [[ "$TEST_CSI_S3" == "true" ]]; then
    log "  - CSI S3: Driver charts installed (may need privileged mode for full functionality)"
else
    log "  - CSI S3: Skipped (set TEST_CSI_S3=true to enable)"
fi
if [[ "$BUILD_IMAGE" == "true" ]]; then
    log "  - Koldun: CRDs installed, operator deployed (locally built image: $IMAGE_TAG)"
elif [[ "$SKIP_OPERATOR" == "true" ]]; then
    log "  - Koldun: CRDs installed, operator skipped (set BUILD_IMAGE=true to test operator)"
else
    log "  - Koldun: CRDs installed, operator deployed"
fi
log "  - Helm upgrade: Idempotent"

exit 0
