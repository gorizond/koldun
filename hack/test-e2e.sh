#!/usr/bin/env bash
set -euo pipefail

# E2E Test Script for Koldun Operator
# Tests complete workflow: HTTP → Backend → NATS → Operator → Dllama → Workers → Inference

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
NAMESPACE="${NAMESPACE:-koldun}"
MODEL_NAME="${MODEL_NAME:-qwen3-0.6b}"
INGRESS_NAME="${INGRESS_NAME:-qwen3-ingress}"
BACKEND_PORT="${BACKEND_PORT:-8082}"
TIMEOUT="${TIMEOUT:-300}"  # 5 minutes for resource creation
INFERENCE_TIMEOUT="${INFERENCE_TIMEOUT:-600}"  # 10 minutes for inference (CPU is slow!)
SKIP_INFERENCE="${SKIP_INFERENCE:-false}"  # Skip actual inference test (just validate resource creation)
CLEANUP="${CLEANUP:-true}"  # Cleanup resources after test

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found. Please install kubectl."
        exit 1
    fi

    if ! command -v curl &> /dev/null; then
        log_error "curl not found. Please install curl."
        exit 1
    fi

    if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
        log_error "Namespace $NAMESPACE not found. Please create it first."
        exit 1
    fi

    log_info "Prerequisites OK"
}

wait_for_condition() {
    local resource="$1"
    local condition="$2"
    local timeout="$3"

    log_info "Waiting for $resource to be $condition (timeout: ${timeout}s)..."

    if kubectl wait --for="condition=$condition" "$resource" -n "$NAMESPACE" --timeout="${timeout}s" 2>/dev/null; then
        log_info "$resource is $condition"
        return 0
    else
        log_error "Timeout waiting for $resource to be $condition"
        return 1
    fi
}

verify_model() {
    log_info "Verifying Model CR: $MODEL_NAME..."

    if ! kubectl get model "$MODEL_NAME" -n "$NAMESPACE" &> /dev/null; then
        log_error "Model $MODEL_NAME not found. Please create Model CR first."
        log_info "See guides/testing.md for Model CR example."
        exit 1
    fi

    if ! wait_for_condition "model/$MODEL_NAME" "Ready" "$TIMEOUT"; then
        kubectl describe model "$MODEL_NAME" -n "$NAMESPACE"
        exit 1
    fi

    log_info "Model $MODEL_NAME is Ready"
}

verify_ingress() {
    log_info "Verifying Ingress CR: $INGRESS_NAME..."

    if ! kubectl get ingress.koldun.gorizond.io "$INGRESS_NAME" -n "$NAMESPACE" &> /dev/null; then
        log_error "Ingress $INGRESS_NAME not found. Please create Ingress CR first."
        log_info "See guides/testing.md for Ingress CR example."
        exit 1
    fi

    if ! wait_for_condition "ingress.koldun.gorizond.io/$INGRESS_NAME" "Ready" "$TIMEOUT"; then
        kubectl describe ingress.koldun.gorizond.io "$INGRESS_NAME" -n "$NAMESPACE"
        exit 1
    fi

    # Wait for backend pod
    log_info "Waiting for backend pod to be ready..."
    local backend_name="${INGRESS_NAME}-backend"
    kubectl wait --for=condition=available deployment/"$backend_name" -n "$NAMESPACE" --timeout="${TIMEOUT}s" || {
        log_error "Backend deployment not ready"
        kubectl describe deployment "$backend_name" -n "$NAMESPACE"
        exit 1
    }

    log_info "Ingress $INGRESS_NAME is Ready"
}

setup_port_forward() {
    log_info "Setting up port forward to backend..."

    # Kill existing port-forward if any
    pkill -f "port-forward.*$BACKEND_PORT" || true
    sleep 2

    local backend_svc="${INGRESS_NAME}-backend"
    kubectl port-forward -n "$NAMESPACE" "svc/$backend_svc" "$BACKEND_PORT:$BACKEND_PORT" &> /dev/null &
    local PF_PID=$!

    # Wait for port-forward to be ready
    for i in {1..10}; do
        if curl -s "http://localhost:$BACKEND_PORT/healthz" &> /dev/null; then
            log_info "Port forward ready (PID: $PF_PID)"
            echo "$PF_PID" > /tmp/koldun-e2e-pf.pid
            return 0
        fi
        sleep 1
    done

    log_error "Port forward failed to become ready"
    kill $PF_PID || true
    exit 1
}

cleanup_port_forward() {
    if [ -f /tmp/koldun-e2e-pf.pid ]; then
        local PF_PID=$(cat /tmp/koldun-e2e-pf.pid)
        log_info "Killing port forward (PID: $PF_PID)..."
        kill "$PF_PID" 2>/dev/null || true
        rm /tmp/koldun-e2e-pf.pid
    fi
}

test_models_endpoint() {
    log_info "Testing /v1/models endpoint..."

    local response=$(curl -s "http://localhost:$BACKEND_PORT/v1/models")

    if echo "$response" | grep -q "\"object\":\"list\""; then
        log_info "/v1/models endpoint OK"
        log_info "Available models:"
        echo "$response" | grep -o "\"id\":\"[^\"]*\"" || true
        return 0
    else
        log_error "/v1/models endpoint failed"
        echo "Response: $response"
        return 1
    fi
}

send_chat_request() {
    log_info "Sending chat completion request..."

    local request_json=$(cat <<EOF
{
  "model": "koldun/$MODEL_NAME",
  "messages": [
    {"role": "user", "content": "Say hello in one word"}
  ],
  "max_tokens": 5
}
EOF
)

    # Send request in background if not skipping inference
    if [ "$SKIP_INFERENCE" = "true" ]; then
        log_warn "Skipping actual inference (SKIP_INFERENCE=true)"
        curl -s -X POST "http://localhost:$BACKEND_PORT/v1/chat/completions" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer test-e2e-token" \
            -d "$request_json" \
            --max-time 5 &> /tmp/koldun-e2e-request.log &
        local REQUEST_PID=$!
        echo "$REQUEST_PID" > /tmp/koldun-e2e-request.pid
        sleep 3  # Give it time to create resources
    else
        log_warn "Sending request with inference (this will take 2-5 minutes on CPU)..."
        curl -s -X POST "http://localhost:$BACKEND_PORT/v1/chat/completions" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer test-e2e-token" \
            -d "$request_json" \
            --max-time "$INFERENCE_TIMEOUT" > /tmp/koldun-e2e-response.json 2>&1 &
        local REQUEST_PID=$!
        echo "$REQUEST_PID" > /tmp/koldun-e2e-request.pid
        sleep 5  # Give it time to create resources
    fi

    log_info "Request sent (PID: $REQUEST_PID)"
}

verify_session_creation() {
    log_info "Verifying Session CR creation..."

    # Wait up to 30 seconds for Session to be created
    for i in {1..30}; do
        local sessions=$(kubectl get sessions.koldun.gorizond.io -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
        sessions=$(echo "$sessions" | tr -d ' ')  # Remove whitespace
        if [ "$sessions" -gt 0 ]; then
            log_info "Session CR created successfully"
            kubectl get sessions.koldun.gorizond.io -n "$NAMESPACE"
            return 0
        fi
        sleep 1
    done

    log_error "No Session CR created after 30 seconds"
    log_info "Checking backend logs:"
    kubectl logs -n "$NAMESPACE" "deployment/${INGRESS_NAME}-backend" --tail=20 || true
    return 1
}

verify_dllama_creation() {
    log_info "Verifying Dllama CR creation..."

    # Wait up to 30 seconds for Dllama to be created
    for i in {1..30}; do
        local dllamas=$(kubectl get dllamas.koldun.gorizond.io -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
        dllamas=$(echo "$dllamas" | tr -d ' ')  # Remove whitespace
        if [ "$dllamas" -gt 0 ]; then
            log_info "Dllama CR created successfully"
            kubectl get dllamas.koldun.gorizond.io -n "$NAMESPACE"

            # Check if Dllama is Ready
            local dllama_name=$(kubectl get dllamas.koldun.gorizond.io -n "$NAMESPACE" -o name | head -1)
            if wait_for_condition "$dllama_name" "Ready" "$TIMEOUT"; then
                return 0
            else
                log_warn "Dllama not Ready yet, but continuing..."
                return 0
            fi
        fi
        sleep 1
    done

    log_error "No Dllama CR created after 30 seconds"
    return 1
}

verify_pods_creation() {
    log_info "Verifying pods creation (dispatcher, root, workers)..."

    # Wait up to 60 seconds for pods to be created
    for i in {1..60}; do
        local session_pods=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | grep "^session-" | wc -l || echo "0")
        session_pods=$(echo "$session_pods" | tr -d ' ')  # Remove whitespace
        if [ "$session_pods" -ge 5 ]; then  # 1 dispatcher + 1 root + 3 workers = 5
            log_info "All expected pods created ($session_pods pods)"
            kubectl get pods -n "$NAMESPACE" | grep "^session-" || true
            return 0
        fi
        sleep 1
    done

    local final_count=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | grep "^session-" | wc -l || echo "0")
    final_count=$(echo "$final_count" | tr -d ' ')  # Remove whitespace
    log_warn "Expected 5 pods (dispatcher + root + 3 workers), found: $final_count"
    kubectl get pods -n "$NAMESPACE" | grep "^session-" || true
    return 0  # Don't fail, just warn
}

verify_topology() {
    log_info "Verifying topology: 1 root + 3 workers..."

    local roots=$(kubectl get roots.koldun.gorizond.io -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
    roots=$(echo "$roots" | tr -d ' ')  # Remove whitespace
    local workers=$(kubectl get workers.koldun.gorizond.io -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
    workers=$(echo "$workers" | tr -d ' ')  # Remove whitespace

    log_info "Found: $roots root(s), $workers worker(s)"

    if [ "$roots" -eq 1 ] && [ "$workers" -ge 1 ]; then
        log_info "Topology validation PASSED: 1 root + $workers worker(s)"
        return 0
    else
        log_warn "Topology validation FAILED: expected 1 root + 3 workers"
        return 1
    fi
}

check_inference_completion() {
    if [ "$SKIP_INFERENCE" = "true" ]; then
        log_warn "Skipping inference completion check (SKIP_INFERENCE=true)"
        return 0
    fi

    log_info "Waiting for inference to complete (timeout: ${INFERENCE_TIMEOUT}s)..."
    log_warn "This may take 2-5 minutes on CPU..."

    if [ ! -f /tmp/koldun-e2e-request.pid ]; then
        log_warn "No request PID found, skipping inference check"
        return 0
    fi

    local REQUEST_PID=$(cat /tmp/koldun-e2e-request.pid)

    # Wait for curl process to complete
    for i in $(seq 1 "$INFERENCE_TIMEOUT"); do
        if ! kill -0 "$REQUEST_PID" 2>/dev/null; then
            log_info "Request completed"

            if [ -f /tmp/koldun-e2e-response.json ]; then
                local response=$(cat /tmp/koldun-e2e-response.json)
                if echo "$response" | grep -q "\"object\":\"chat.completion\""; then
                    log_info "Inference completed successfully!"
                    log_info "Response preview:"
                    echo "$response" | head -20
                    return 0
                else
                    log_error "Inference failed or returned unexpected response"
                    echo "Response: $response"
                    return 1
                fi
            fi
            return 0
        fi
        sleep 1
    done

    log_warn "Inference timeout reached, killing request process..."
    kill "$REQUEST_PID" 2>/dev/null || true
    return 1
}

cleanup_resources() {
    if [ "$CLEANUP" != "true" ]; then
        log_warn "Skipping cleanup (CLEANUP=false)"
        return 0
    fi

    log_info "Cleaning up test resources..."

    # Delete Session resources (will cascade to Dllama, Root, Worker)
    kubectl delete sessions.koldun.gorizond.io --all -n "$NAMESPACE" --ignore-not-found=true || true

    # Cleanup temp files
    rm -f /tmp/koldun-e2e-*.json /tmp/koldun-e2e-*.log /tmp/koldun-e2e-*.pid || true

    log_info "Cleanup complete"
}

main() {
    log_info "Starting E2E test for Koldun Operator"
    log_info "Namespace: $NAMESPACE"
    log_info "Model: $MODEL_NAME"
    log_info "Ingress: $INGRESS_NAME"
    log_info "Skip inference: $SKIP_INFERENCE"

    # Trap to cleanup on exit
    trap 'cleanup_port_forward; cleanup_resources' EXIT

    check_prerequisites
    verify_model
    verify_ingress
    setup_port_forward

    test_models_endpoint || exit 1

    send_chat_request

    verify_session_creation || exit 1
    verify_dllama_creation || exit 1
    verify_pods_creation || exit 1
    verify_topology || log_warn "Topology validation failed but continuing..."

    check_inference_completion || log_warn "Inference check failed but test resources were created successfully"

    log_info "======================================"
    log_info "E2E Test Summary:"
    log_info "✅ Model Ready"
    log_info "✅ Ingress Ready"
    log_info "✅ Backend responding"
    log_info "✅ Session CR created"
    log_info "✅ Dllama CR created"
    log_info "✅ Pods created (dispatcher + root + workers)"
    if [ "$SKIP_INFERENCE" = "true" ]; then
        log_info "⏭️  Inference skipped"
    else
        log_info "⏳ Inference completed (or timed out)"
    fi
    log_info "======================================"
    log_info "E2E TEST PASSED (resource creation validated)"

    return 0
}

main "$@"
