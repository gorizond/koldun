.PHONY: clean test coverage coverage-clean envtest-preflight controllers-smoke

ENVTEST_CONTROLLER_RUNTIME_VERSION ?= 0.20.4
ENVTEST_K8S_VERSION ?= 1.32.x!
ENVTEST_DIR ?= ./bin/envtest
PRINT_ENVTEST_ASSETS := ./hack/print-kubebuilder-assets.sh
SETUP_ENVTEST_BIN ?= $(shell command -v setup-envtest 2>/dev/null || printf "%s" "$(HOME)/go/bin/setup-envtest")

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
# See reasoning/guides/testing.md "Envtest Hanging Issue" section for details
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
	go test ./pkg/controllers -count=1 -timeout=5m
	@echo "✓ All controller tests passed"

# Verify individual test packages work (no deadlock in smaller test suites)
test-quick:
	@echo "Running quick test suite (excludes controllers)..."
	@go test ./cmd/... ./pkg/api/... ./pkg/clients/... ./pkg/registry/... -timeout=2m
	@echo "✓ Quick tests passed"
