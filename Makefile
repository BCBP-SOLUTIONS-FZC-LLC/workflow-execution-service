SHELL := /bin/bash

# Auto-load .env if present so make targets pick up DATABASE_URL etc. without
# requiring `source .env` in the shell first.
ifneq ($(wildcard .env),)
  include .env
  export
endif

SQLC_VERSION         := v1.31.1
BUF_VERSION          := v1.50.0
MOCKGEN_VERSION      := v0.6.0
GOLANGCI_VERSION     := v2.12.2
GOVULNCHECK_VERSION  := v1.1.4
GOARCHLINT_VERSION   := v1.15.0
HADOLINT_VERSION     := v2.12.0
TRIVY_VERSION        := 0.71.2

# Docker images pulled by integration tests via testcontainers-go.
# Run `make tools-integration` once to warm the local Docker image cache.
TESTCONTAINERS_POSTGRES_IMAGE    := postgres:18-alpine
TESTCONTAINERS_LOCALSTACK_IMAGE  := localstack/localstack:3
TESTCONTAINERS_VALKEY_IMAGE      := valkey/valkey:8-alpine

TOOLS_DIR          := .tools
BIN_DIR            := bin
COVERAGE_DIR       := .coverage
MODULE             := github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service

SQLC               := $(TOOLS_DIR)/sqlc
BUF                := $(TOOLS_DIR)/buf
MOCKGEN            := $(TOOLS_DIR)/mockgen
GOLANGCI           := $(TOOLS_DIR)/golangci-lint
GOVULNCHECK        := $(TOOLS_DIR)/govulncheck
GOARCHLINT         := $(TOOLS_DIR)/go-arch-lint
HADOLINT           := $(TOOLS_DIR)/hadolint
TRIVY              := $(TOOLS_DIR)/trivy

BUILD_VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS            := -X main.version=$(BUILD_VERSION)

COVER_PROFILE      := $(COVERAGE_DIR)/coverage.out
COVER_HTML         := $(COVERAGE_DIR)/coverage.html
# Exclude generated packages (sqlc db/, mockgen mocks/) from the coverage denominator.
COVER_EXCLUDE_PKG  := /postgres/db$$\|/mocks$$
COVER_EXCLUDE_FILE := /postgres/db/\|/mocks/
COVER_THRESHOLD    := 95
# Per-package floors: packages not listed must meet COVER_THRESHOLD.
# internal/workflow (the Temporal interpreter) sits at ~91.3% isolated
# unit-test coverage — SLA-timer/replay-only paths make the last few points
# expensive to close without a live Temporal test environment. Floored a few
# points under the measured level, not at it. Add an entry here only for a
# package that's genuinely hard to exercise at the global bar, never to
# paper over a gap in new code.
COVER_PKG_FLOORS   := internal/workflow:88

.PHONY: all tools tools-integration generate generate-proto generate-sqlc mock \
        build migrate dev test test-integration test-ci merge-coverage \
        cover cover-func cover-gaps cover-html cover-check cover-check-pkg \
        arch-lint lint lint-fix vuln \
        tidy fmt-check fix check \
        setup install-hooks \
        docker-up docker-down \
        docker-build docker-build-server docker-build-worker docker-lint docker-trivy docker-check pin-base-images \
        clean help

all: generate build


## tools: Install all dev tooling into .tools/
tools:
	@mkdir -p $(TOOLS_DIR)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install go.uber.org/mock/mockgen@$(MOCKGEN_VERSION)
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh \
		| sh -s -- -b $(PWD)/$(TOOLS_DIR) $(GOLANGCI_VERSION)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install github.com/fe3dback/go-arch-lint@$(GOARCHLINT_VERSION)
	@OS=$$(uname -s); ARCH=$$(uname -m); \
	if [ "$$OS" = "Darwin" ]; then \
	    if command -v brew >/dev/null 2>&1; then \
	        brew install hadolint >/dev/null && cp "$$(brew --prefix)/bin/hadolint" $(TOOLS_DIR)/hadolint; \
	    else \
	        echo "hadolint: brew not found on macOS — install manually: brew install hadolint"; exit 1; \
	    fi; \
	else \
	    curl -sLo $(TOOLS_DIR)/hadolint \
	      "https://github.com/hadolint/hadolint/releases/download/$(HADOLINT_VERSION)/hadolint-$$OS-$$ARCH" && \
	    chmod +x $(TOOLS_DIR)/hadolint; \
	fi
	@OS=$$(uname -s); ARCH=$$(uname -m); \
	if [ "$$OS" = "Darwin" ] && [ "$$ARCH" = "arm64" ]; then TRIVY_OS_ARCH="macOS-ARM64"; \
	elif [ "$$OS" = "Darwin" ]; then TRIVY_OS_ARCH="macOS-64bit"; \
	elif [ "$$ARCH" = "aarch64" ] || [ "$$ARCH" = "arm64" ]; then TRIVY_OS_ARCH="Linux-ARM64"; \
	else TRIVY_OS_ARCH="Linux-64bit"; fi; \
	curl -sLo /tmp/trivy.tar.gz \
	  "https://github.com/aquasecurity/trivy/releases/download/v$(TRIVY_VERSION)/trivy_$(TRIVY_VERSION)_$$TRIVY_OS_ARCH.tar.gz" && \
	tar -xzf /tmp/trivy.tar.gz -C $(TOOLS_DIR) trivy && rm /tmp/trivy.tar.gz
	@echo "✓ tools installed to $(TOOLS_DIR)/"

## tools-integration: Pre-pull Docker images used by integration tests (testcontainers-go)
tools-integration:
	docker pull $(TESTCONTAINERS_POSTGRES_IMAGE)
	docker pull $(TESTCONTAINERS_LOCALSTACK_IMAGE)
	docker pull $(TESTCONTAINERS_VALKEY_IMAGE)
	@echo "✓ Docker images ready for integration tests"


## generate: Run buf (proto → gen/) and sqlc (queries → postgres/db/)
generate: generate-proto generate-sqlc

generate-proto:
	@echo "→ buf generate"
	$(BUF) generate
	@echo "proto stubs written to gen/proto/workflow/{definition,execution}/v1/"

generate-sqlc:
	@echo "→ sqlc generate"
	$(SQLC) generate
	@echo "sqlc output written to internal/adapter/outbound/postgres/db/"

## mock: Regenerate GoMock stubs for all core/port interfaces
mock:
	@mkdir -p internal/core/port/mocks
	$(MOCKGEN) -destination=internal/core/port/mocks/repository.go -package=mocks \
		$(MODULE)/internal/core/port InstanceRepository,TaskRepository,TaskAssignmentRepository
	$(MOCKGEN) -destination=internal/core/port/mocks/outbox.go -package=mocks \
		$(MODULE)/internal/core/port OutboxRepository
	$(MOCKGEN) -destination=internal/core/port/mocks/processed_event.go -package=mocks \
		$(MODULE)/internal/core/port ProcessedEventRepository
	$(MOCKGEN) -destination=internal/core/port/mocks/transactor.go -package=mocks \
		$(MODULE)/internal/core/port Transactor


## build: Compile the server and worker binaries to bin/
build:
	@mkdir -p $(BIN_DIR)
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/server ./cmd/server
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/worker ./cmd/worker
	@echo "✓ binaries: $(BIN_DIR)/server, $(BIN_DIR)/worker"

## migrate: Apply DB schema migrations (outbox + domain) and exit. Run before the server/worker.
migrate:
	go run ./cmd/server migrate

## dev: Run the server locally (requires .env to be populated).
dev:
	go run ./cmd/server

## test: Run unit tests with race detector and coverage (internal + test/unit)
test:
	@mkdir -p $(COVERAGE_DIR)
	go test -race -count=1 \
	    -coverpkg=$$(go list ./internal/... ./cmd/... | grep -v '$(COVER_EXCLUDE_PKG)' | tr '\n' ',' | sed 's/,$$//') \
	    -coverprofile=$(COVERAGE_DIR)/unit.out -covermode=atomic \
	    ./internal/... ./test/unit/...
	@grep -v '$(COVER_EXCLUDE_FILE)' $(COVERAGE_DIR)/unit.out > $(COVERAGE_DIR)/unit.out.filtered && mv $(COVERAGE_DIR)/unit.out.filtered $(COVERAGE_DIR)/unit.out
	@go tool cover -func=$(COVERAGE_DIR)/unit.out | awk '/^total:/{print "total:", $$NF}'

## test-integration: Run integration tests — spins up containers via testcontainers-go (no make docker-up needed)
test-integration:
	@mkdir -p $(COVERAGE_DIR)
	AWS_ACCESS_KEY_ID=test \
	AWS_SECRET_ACCESS_KEY=test \
	AWS_EC2_METADATA_DISABLED=true \
	TESTCONTAINERS_RYUK_DISABLED=true \
	go test -race -count=1 -tags integration \
	    -coverpkg=$$(go list ./internal/... | grep -v '$(COVER_EXCLUDE_PKG)' | tr '\n' ',' | sed 's/,$$//') \
	    -coverprofile=$(COVERAGE_DIR)/integration.out \
	    -covermode=atomic \
	    ./test/integration/...
	@grep -v '$(COVER_EXCLUDE_FILE)' $(COVERAGE_DIR)/integration.out > $(COVERAGE_DIR)/integration.out.filtered && mv $(COVERAGE_DIR)/integration.out.filtered $(COVERAGE_DIR)/integration.out
	@go tool cover -func=$(COVERAGE_DIR)/integration.out | tail -1

## merge-coverage: Merge unit + integration profiles into coverage.out (max-count-per-block strategy)
merge-coverage:
	@python3 scripts/merge_coverage.py $(COVERAGE_DIR)/unit.out $(COVERAGE_DIR)/integration.out > $(COVER_PROFILE)
	@echo "✓ $(COVER_PROFILE) merged from unit + integration suites"

## test-ci: Run unit + integration suites and merge coverage
test-ci: test test-integration merge-coverage
	@go tool cover -func=$(COVER_PROFILE) | awk '/^total:/{print "total:", $$NF}'

## cover: Print total coverage from last test run (run 'make test-ci' first)
cover:
	@[ -f $(COVER_PROFILE) ] || { echo "no profile — run 'make test-ci' first"; exit 1; }
	@go tool cover -func=$(COVER_PROFILE) | awk '/^total:/{print "total:", $$NF}'

## cover-func: Print per-function coverage breakdown (run 'make test-ci' first)
cover-func:
	@[ -f $(COVER_PROFILE) ] || { echo "no profile — run 'make test-ci' first"; exit 1; }
	@go tool cover -func=$(COVER_PROFILE)

## cover-gaps: Show uncovered and partially-covered functions (run 'make test-ci' first)
cover-gaps:
	@[ -f $(COVER_PROFILE) ] || { echo "no profile — run 'make test-ci' first"; exit 1; }
	@./scripts/uncovered.sh $(COVER_PROFILE)

## cover-html: Open HTML coverage report in the browser (run 'make test-ci' first)
cover-html:
	@[ -f $(COVER_PROFILE) ] || { echo "no profile — run 'make test-ci' first"; exit 1; }
	go tool cover -html=$(COVER_PROFILE) -o $(COVER_HTML)
	@echo "✓ report: $(COVER_HTML)"
	@open $(COVER_HTML) 2>/dev/null || xdg-open $(COVER_HTML) 2>/dev/null || true

## cover-check-pkg: Per-package coverage gate — each package must meet its floor (run 'make test-ci' first)
cover-check-pkg:
	@[ -f $(COVER_PROFILE) ] || { echo "no profile — run 'make test-ci' first"; exit 1; }
	@awk \
	  -v module="$(MODULE)/" \
	  -v floors="$(subst \,,$(COVER_PKG_FLOORS))" \
	  -v global="$(COVER_THRESHOLD)" \
	  'BEGIN { \
	    n=split(floors,pairs," "); \
	    for(i=1;i<=n;i++){split(pairs[i],kv,":");thresh[kv[1]]=kv[2]+0} \
	  } \
	  /^mode:/{next} \
	  { \
	    key=$$1; stmts=$$2+0; count=$$3+0; \
	    blk_stmts[key]=stmts; blk_count[key]+=count; \
	    path=key; sub(/:.*$$/,"",path); sub(module,"",path); sub(/\/[^\/]+$$/,"",path); \
	    blk_pkg[key]=path \
	  } \
	  END { \
	    for(key in blk_stmts){ \
	      pkg=blk_pkg[key]; \
	      tot[pkg]+=blk_stmts[key]; \
	      if(blk_count[key]>0) cov[pkg]+=blk_stmts[key] \
	    } \
	    fail=0; \
	    for(pkg in tot){ \
	      if(tot[pkg]==0)continue; \
	      pct=cov[pkg]*100/tot[pkg]; \
	      floor=(pkg in thresh)?thresh[pkg]:global; \
	      if(pct<floor){printf "✗  %-58s %5.1f%% (need %d%%)\n",pkg,pct,floor; fail=1} \
	      else{printf "✓  %-58s %5.1f%%\n",pkg,pct} \
	    } \
	    exit fail \
	  }' $(COVER_PROFILE)

## cover-check: Per-package coverage gate (runs unit + integration tests automatically)
cover-check: test-ci cover-check-pkg
	@go tool cover -func=$(COVER_PROFILE) | awk '/^total:/{print "total (informational):", $$NF}'
	@echo "✓ coverage ok"


## tidy: Run go mod tidy
tidy:
	go mod tidy

## fmt-check: Verify gofmt formatting (read-only; exits non-zero on violations)
fmt-check:
	@files=$$(gofmt -l cmd/ internal/ test/); if [ -n "$$files" ]; then echo "gofmt violations (run 'make fix'):"; echo "$$files"; exit 1; fi

## fix: Auto-fix formatting (gofmt) and lint issues (golangci-lint --fix)
fix:
	@echo "==> gofmt (auto-fix)"
	gofmt -w cmd/ internal/ test/
	@echo "==> lint (auto-fix)"
	$(GOLANGCI) run --fix ./...
	@echo "✓ formatting and lint fixes applied"

## check: Verify formatting/lint (read-only), run vet, tests, and coverage gate — full local CI pass
check:
	@echo "==> gofmt"
	@$(MAKE) fmt-check
	@echo "==> lint"
	$(GOLANGCI) run ./cmd/... ./internal/... ./test/...
	@echo "==> go vet"
	go vet ./cmd/... ./internal/...
	@echo "==> arch-lint"
	$(MAKE) arch-lint
	@echo "==> test-ci (unit + integration, merged coverage gate)"
	$(MAKE) cover-check
	@echo "✓ all checks passed"

## arch-lint: Enforce Clean Architecture import direction via go-arch-lint
arch-lint:
	$(GOARCHLINT) check --project-path .

## lint: Run golangci-lint (read-only; exits non-zero on violations)
lint:
	$(GOLANGCI) run ./cmd/... ./internal/... ./test/...

## lint-fix: Run golangci-lint with auto-fix
lint-fix:
	$(GOLANGCI) run --fix ./cmd/... ./internal/... ./test/...

## vuln: Run govulncheck to detect known vulnerabilities in dependencies
vuln:
	$(GOVULNCHECK) ./...


IMAGE_TAG ?= local

## docker-build: Build both the server and worker container images (requires GO_PRIVATE_TOKEN in env)
docker-build: docker-build-server docker-build-worker

## docker-build-server: Build only the cmd/server image (--target server)
docker-build-server:
	docker buildx build \
	  --target server \
	  --secret id=go_private_token,env=GO_PRIVATE_TOKEN \
	  --build-arg BUILD_VERSION=$(IMAGE_TAG) \
	  --load \
	  -t execution-service-server:$(IMAGE_TAG) .

## docker-build-worker: Build only the cmd/worker image (--target worker)
docker-build-worker:
	docker buildx build \
	  --target worker \
	  --secret id=go_private_token,env=GO_PRIVATE_TOKEN \
	  --build-arg BUILD_VERSION=$(IMAGE_TAG) \
	  --load \
	  -t execution-service-worker:$(IMAGE_TAG) .

## docker-lint: Lint Dockerfile with Hadolint (run 'make tools' to install)
docker-lint:
	$(HADOLINT) --config .hadolint.yaml Dockerfile

## docker-trivy: Scan source and dependencies for HIGH/CRITICAL CVEs (run 'make tools' to install)
docker-trivy:
	$(TRIVY) fs . \
	  --severity HIGH,CRITICAL \
	  --ignore-unfixed \
	  --exit-code 1 \
	  --skip-dirs vendor \
	  --skip-dirs platform-libs

## docker-check: Run Dockerfile lint + dependency CVE scan (no image build required)
docker-check: docker-lint docker-trivy
	@echo "✓ all container checks passed"

## pin-base-images: Resolve current digests for Dockerfile base images and pin them (writes .docker-digests)
pin-base-images:
	@GOLANG_DIGEST=$$(docker buildx imagetools inspect golang:1.26-alpine | awk '/^Digest:/{print $$2; exit}'); \
	DISTROLESS_DIGEST=$$(docker buildx imagetools inspect gcr.io/distroless/static-debian12:nonroot | awk '/^Digest:/{print $$2; exit}'); \
	sed -i.bak "s|FROM golang:1.26-alpine.*AS builder|FROM golang:1.26-alpine@$$GOLANG_DIGEST AS builder|" Dockerfile; \
	sed -i.bak "s|FROM gcr.io/distroless/static-debian12:nonroot.*|FROM gcr.io/distroless/static-debian12:nonroot@$$DISTROLESS_DIGEST|" Dockerfile; \
	rm -f Dockerfile.bak; \
	printf 'golang:1.26-alpine@%s\ngcr.io/distroless/static-debian12:nonroot@%s\n' "$$GOLANG_DIGEST" "$$DISTROLESS_DIGEST" > .docker-digests; \
	echo "Pinned base images — see .docker-digests"


## setup: First-time onboarding — copy .env.example → .env and install git hooks
setup:
	@test -f .env || cp .env.example .env
	@mkdir -p .git/hooks
	@cp .githooks/pre-commit .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "✓ Environment ready (.env) and git hooks installed"

## install-hooks: (Re)install the local pre-commit hook — run after .githooks/pre-commit changes
install-hooks:
	@mkdir -p .git/hooks
	@cp .githooks/pre-commit .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "✓ Installed git hooks"

## docker-up: Start local infra (Postgres + Valkey + LocalStack + PgBouncer + Temporal dev server)
# Host ports are offset from definition_service's docker-compose.yml so both
# stacks can run side by side for local cross-service testing.
docker-up:
	docker compose up -d
	@echo "postgres on :5433, valkey on :6380, localstack on :4567, pgbouncer on :6433, temporal on :7233 (UI :8233)"

## docker-down: Stop local infra
docker-down:
	docker compose down


## clean: Remove build output, generated files, and coverage reports
clean:
	rm -rf $(BIN_DIR)
	rm -rf $(COVERAGE_DIR)
	rm -rf gen/
	rm -rf internal/adapter/outbound/postgres/db/
	rm -rf internal/core/port/mocks/


## help: List available targets
help:
	@grep -E '^## ' Makefile | sed 's/## /  /'
