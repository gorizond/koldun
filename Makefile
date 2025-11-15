.PHONY: clean test coverage coverage-clean envtest-preflight controllers-smoke controllers-coverage-check compose-test-up compose-test-down compose-test compose-update-baseline help

SHELL := /bin/bash

ENVTEST_CONTROLLER_RUNTIME_VERSION ?= 0.20.4
ENVTEST_K8S_VERSION ?= 1.32.x!
ENVTEST_DIR ?= ./bin/envtest
PRINT_ENVTEST_ASSETS := ./hack/print-kubebuilder-assets.sh
SETUP_ENVTEST_BIN ?= $(shell command -v setup-envtest 2>/dev/null || printf "%s" "$(HOME)/go/bin/setup-envtest")
COMPOSE_FILE ?= docker-compose.test.yml
COMPOSE_TEST_LOG_PATH ?= artifacts/compose-logs.txt
COMPOSE_TEST_COVERPROFILE ?= compose.coverprofile
COMPOSE_TEST_BASELINE ?= analytics/compose_coverage_baseline.json

help:
	@echo "Available targets:"
	@printf "  %-25s %s\n" "test" "Run go test ./..."
	@printf "  %-25s %s\n" "coverage" "Generate coverage.out and HTML report"
	@printf "  %-25s %s\n" "envtest-preflight" "Download and validate envtest assets"
	@printf "  %-25s %s\n" "controllers-smoke" "Run controller envtest suite without coverage"
	@printf "  %-25s %s\n" "controllers-coverage-check" "Verify controllers coverage >= 99% (CI gate)"
	@printf "  %-25s %s\n" "compose-test" "Spin up docker compose stack and run dispatcher/ingress tests"
	@printf "  %-25s %s\n" "compose-update-baseline" "Update analytics/compose_coverage_baseline.json from compose.coverprofile"

# Clean temporary files generated during testing and coverage
clean:
	@echo "Cleaning temporary files..."
	@rm -f *.out
	@rm -f coverage.func
	@rm -f *.coverprofile
	@rm -f *_coverage.out
	@rm -f coverage.html
	@rm -f *.test
	@find . -name "*.out" -type f -delete
	@echo "✓ Cleanup complete"

# Run tests
test:
	go test ./...

# Run tests with coverage
coverage:
	@echo "Running tests with coverage..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "✓ Coverage report generated: coverage.html"

# Run coverage and then cleanup
coverage-clean: coverage clean

# Prepare controller-runtime envtest assets and print KUBEBUILDER_ASSETS export
envtest-preflight:
	@if [ ! -x "$(SETUP_ENVTEST_BIN)" ]; then \
		echo "setup-envtest not found at $(SETUP_ENVTEST_BIN)."; \
		echo "Install with: go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest"; \
		exit 1; \
	fi
	@{ \
		echo "Downloading envtest assets (k8s $(ENVTEST_K8S_VERSION), controller-runtime $(ENVTEST_CONTROLLER_RUNTIME_VERSION))..."; \
		eval "$$($(SETUP_ENVTEST_BIN) use -p env --bin-dir $(ENVTEST_DIR) $(ENVTEST_K8S_VERSION))"; \
		ASSETS="$${KUBEBUILDER_ASSETS:-}"; \
		if [ -z "$$ASSETS" ]; then \
			if ! ASSETS="$$( $(PRINT_ENVTEST_ASSETS) )"; then \
				echo "Unable to detect KUBEBUILDER_ASSETS. Run setup-envtest manually."; \
				exit 1; \
			fi; \
		fi; \
		if [ ! -f "$$ASSETS/kube-apiserver" ]; then \
			echo "kube-apiserver binary missing under $$ASSETS"; \
			exit 1; \
		fi; \
		if [ ! -f "$$ASSETS/etcd" ]; then \
			echo "etcd binary missing under $$ASSETS"; \
			exit 1; \
		fi; \
		echo "✓ KUBEBUILDER_ASSETS ready at $$ASSETS"; \
		echo "Add to your environment (e.g. direnv, shell profile):"; \
		echo "  export KUBEBUILDER_ASSETS=\"$$ASSETS\""; \
	}

# Run controller tests WITHOUT coverage (avoids t.Parallel + coverprofile deadlock)
# See context/guides/testing.md "Envtest Hanging Issue" section for details
controllers-smoke:
	@echo "Running controller tests..."
	@ASSETS="$${KUBEBUILDER_ASSETS:-}"; \
	if [ -z "$$ASSETS" ]; then \
		if ! ASSETS="$$( $(PRINT_ENVTEST_ASSETS) 2>/dev/null )"; then \
			echo "KUBEBUILDER_ASSETS not configured. Run 'make envtest-preflight' first."; \
			exit 1; \
		fi; \
	fi; \
	echo "Using KUBEBUILDER_ASSETS=$$ASSETS"; \
	export KUBEBUILDER_ASSETS="$$ASSETS"; \
	go test ./pkg/controllers -count=1 -timeout=10m
	@echo "✓ All controller tests passed"

# Verify controllers coverage meets minimum threshold (99%)
# Used as CI gate to prevent coverage regression
controllers-coverage-check:
	@./hack/check-controller-coverage.sh

# Verify individual test packages work (no deadlock in smaller test suites)
test-quick:
	@echo "Running quick test suite (excludes controllers)..."
	@go test ./cmd/... ./pkg/api/... ./pkg/clients/... ./pkg/registry/... -timeout=2m
	@echo "✓ Quick tests passed"

compose-test-up:
	docker compose -f $(COMPOSE_FILE) up -d

compose-test-down:
	docker compose -f $(COMPOSE_FILE) down -v

compose-test:
	@set -euo pipefail; \
	LOG_PATH="$${COMPOSE_TEST_LOG_PATH:-artifacts/compose-logs.txt}"; \
	COVER_PATH="$${COMPOSE_TEST_COVERPROFILE:-compose.coverprofile}"; \
	mkdir -p "$$(dirname "$$LOG_PATH")"; \
	mkdir -p "$$(dirname "$$COVER_PATH")"; \
	: > "$$LOG_PATH"; \
	CACHE_DIR="$$(mktemp -d)"; \
	KEEP_STACK="$${COMPOSE_TEST_KEEP_STACK:-}"; \
	trap 'CODE=$$?; docker compose -f $(COMPOSE_FILE) logs --no-color > "$$LOG_PATH" 2>&1 || true; if [ -z "$$KEEP_STACK" ]; then $(MAKE) compose-test-down >/dev/null 2>&1 || true; else echo "COMPOSE_TEST_KEEP_STACK=1 set; leaving compose stack running" >&2; fi; rm -rf "$$CACHE_DIR"; exit $$CODE' EXIT; \
	$(MAKE) compose-test-up >/dev/null; \
	./hack/localstack/wait-for-stack.sh 180; \
	NATS_URL="$${KOLDUN_NATS_URL:-nats://koldun:koldun@127.0.0.1:4222}"; \
	export KOLDUN_NATS_URL="$$NATS_URL"; \
	if [ -z "$${KOLDUN_DISPATCHER_NATS_URL:-}" ]; then \
		export KOLDUN_DISPATCHER_NATS_URL="$$NATS_URL"; \
	fi; \
	GOCACHE="$$CACHE_DIR" go test -count=1 -coverprofile="$$COVER_PATH" ./pkg/servers/ingress ./pkg/servers/dispatcher

compose-update-baseline:
	@./hack/update-compose-coverage-baseline.sh "$(COMPOSE_TEST_COVERPROFILE)" "$(COMPOSE_TEST_BASELINE)"
