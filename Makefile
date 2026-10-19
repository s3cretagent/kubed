# kubed build tasks. `make help` lists them.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: help build install test smoke lint prices clean

help: ## Show targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

build: ## Build ./bin/kubed
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/kubed ./cmd/kubed

install: ## Install kubed into $GOPATH/bin
	CGO_ENABLED=0 go install -ldflags "$(LDFLAGS)" ./cmd/kubed

test: ## Unit tests with the race detector
	go test -race ./...

smoke: ## Read-only smoke test against a real cluster: make smoke CONTEXT=<kube-context>
	@test -n "$(CONTEXT)" || (echo "usage: make smoke CONTEXT=<kube-context>" && exit 1)
	KUBED_SMOKE_CONTEXT=$(CONTEXT) go test -tags smoke -run TestSmoke -count=1 -v ./internal/ui

lint: ## gofmt + go vet + golangci-lint
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "run gofmt -w ." && exit 1)
	go vet ./...
	golangci-lint run

prices: ## Regenerate the builtin price list from instances.vantage.sh
	go generate ./internal/pricing

clean: ## Remove build output
	rm -rf bin dist
