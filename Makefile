COVERAGE_FILE = coverage.txt
GENERATED_CODE_DIRS = "/mock/"
COVERAGE_NO_GEN_FILE = coverage_no_gen.txt

.PHONY: test
test:
	@go test -race -failfast -coverprofile $(COVERAGE_FILE) -coverpkg ./... ./...
	@echo "\033[32m-- Test coverage\033[0m"
	@cat $(COVERAGE_FILE) | egrep -v $(GENERATED_CODE_DIRS) > $(COVERAGE_NO_GEN_FILE)
	@go tool cover -func=$(COVERAGE_NO_GEN_FILE)|grep "total:"

