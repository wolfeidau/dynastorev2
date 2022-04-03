GOLANGCI_VERSION = 1.45.2

ci: lint test
.PHONY: ci

lint:
	@docker run --rm -v $(shell pwd):/app -w /app golangci/golangci-lint:v$(GOLANGCI_VERSION) golangci-lint run -v
.PHONY: lint

test: 
	@go test -v -covermode=count -coverprofile=coverage.txt ./
.PHONY: test