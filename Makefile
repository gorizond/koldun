.PHONY: clean test coverage coverage-clean envtest-preflight controllers-smoke

ENVTEST_VERSION ?= 0.20.4
ENVTEST_DIR ?= ./bin/envtest
ENVTEST_ASSETS := $(ENVTEST_DIR)/kubebuilder/bin

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
	@command -v setup-envtest >/dev/null 2>&1 || { \
		echo "setup-envtest not found."; \
		echo "Install with: go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest"; \
		exit 1; \
	}
	@echo "Downloading envtest assets (controller-runtime $(ENVTEST_VERSION))..."
	@eval "$$(setup-envtest use --controller-runtime-version $(ENVTEST_VERSION) --install-dir $(ENVTEST_DIR))"
	@[ -f "$(ENVTEST_ASSETS)/kube-apiserver" ] || { \
		echo "kube-apiserver binary missing under $(ENVTEST_ASSETS)"; \
		exit 1; \
	}
	@[ -f "$(ENVTEST_ASSETS)/etcd" ] || { \
		echo "etcd binary missing under $(ENVTEST_ASSETS)"; \
		exit 1; \
	}
	@echo "✓ KUBEBUILDER_ASSETS ready at $(PWD)/$(ENVTEST_ASSETS)"
	@echo "Add to your environment (e.g. direnv, shell profile):"
	@echo "  export KUBEBUILDER_ASSETS=\"$(PWD)/$(ENVTEST_ASSETS)\""

# Run controller tests WITHOUT coverage (avoids t.Parallel + coverprofile deadlock)
# See reasoning/guides/testing.md "Envtest Hanging Issue" section for details
controllers-smoke:
	@echo "Running controller tests..."
	@export KUBEBUILDER_ASSETS="$(PWD)/bin/envtest/k8s/1.32.0-darwin-arm64" && \
	go test ./pkg/controllers -count=1 -timeout=5m
	@echo "✓ All controller tests passed"

# Verify individual test packages work (no deadlock in smaller test suites)
test-quick:
	@echo "Running quick test suite (excludes controllers)..."
	@go test ./cmd/... ./pkg/api/... ./pkg/clients/... ./pkg/registry/... -timeout=2m
	@echo "✓ Quick tests passed"
