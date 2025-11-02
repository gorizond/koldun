.PHONY: clean test coverage

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
