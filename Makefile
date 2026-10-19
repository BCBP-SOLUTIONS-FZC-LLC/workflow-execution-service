SHELL := /bin/bash

# Auto-load .env if present so make targets pick up DATABASE_URL etc. without
# requiring `source .env` in the shell first.
ifneq ($(wildcard .env),)
  include .env
  export
endif

SQLC_VERSION         := v1.31.1
OASDIFF_VERSION      := v1.32.1
MOCKGEN_VERSION      := v0.6.0
GOLANGCI_VERSION     := v2.12.2
GOVULNCHECK_VERSION  := v1.1.4
GOARCHLINT_VERSION   := v1.15.0
HADOLINT_VERSION     := v2.12.0
TRIVY_VERSION        := 0.71.2
KUBECONFORM_VERSION  := 0.8.0
ACTIONLINT_VERSION   := v1.7.12

# Docker images pulled by integration tests via testcontainers-go.
# Run `make tools-integration` once to warm the local Docker image cache.
TESTCONTAINERS_POSTGRES_IMAGE    := postgres:18-alpine
TESTCONTAINERS_LOCALSTACK_IMAGE  := localstack/localstack:3
TESTCONTAINERS_VALKEY_IMAGE      := valkey/valkey:8-alpine
TESTCONTAINERS_TEMPORAL_IMAGE    := temporalio/temporal:latest

TOOLS_DIR          := .tools
BIN_DIR            := bin
COVERAGE_DIR       := .coverage
MODULE             := github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service

SQLC               := $(TOOLS_DIR)/sqlc
OASDIFF            := $(TOOLS_DIR)/oasdiff
MOCKGEN            := $(TOOLS_DIR)/mockgen
GOLANGCI           := $(TOOLS_DIR)/golangci-lint
GOVULNCHECK        := $(TOOLS_DIR)/govulncheck
GOARCHLINT         := $(TOOLS_DIR)/go-arch-lint
HADOLINT           := $(TOOLS_DIR)/hadolint
TRIVY              := $(TOOLS_DIR)/trivy
KUBECONFORM        := $(TOOLS_DIR)/kubeconform
ACTIONLINT         := $(TOOLS_DIR)/actionlint
HELM               ?= helm

BUILD_VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS            := -X main.version=$(BUILD_VERSION)

COVER_PROFILE      := $(COVERAGE_DIR)/coverage.out
COVER_HTML         := $(COVERAGE_DIR)/coverage.html
# Exclude generated packages (sqlc db/, mockgen mocks/) and the two
# composition roots with no tests of their own (cmd/server, cmd/worker) from
# the coverage denominator.
COVER_EXCLUDE_PKG  := /postgres/db$$\|/mocks$$\|/obstest$$\|/cmd/server$$\|/cmd/worker$$
COVER_EXCLUDE_FILE := /postgres/db/\|/mocks/\|/obstest/\|/cmd/server/\|/cmd/worker/
COVER_THRESHOLD    := 95
# Per-package floors: packages not listed must meet COVER_THRESHOLD.
# internal/workflow (the Temporal interpreter) sits at ~91.3% isolated
# unit-test coverage — SLA-timer/replay-only paths make the last few points
# expensive to close without a live Temporal test environment. Floored a few
# points under the measured level, not at it. Add an entry here only for a
# package that's genuinely hard to exercise at the global bar, never to
# paper over a gap in new code.
# cmd/connector-worker (~58.9%) holds its dispatch logic, unit-tested, beside
# its composition root (buildDeps, main, the read/reclaim/heartbeat loops,
# the health server), which only `make test-e2e` exercises.
# internal/observability (~92%) keeps the error-wrapping branches of the
# telemetry initialisation, which fail only when a platform library refuses
# its own identity, and the Search Attribute upsert failure.
COVER_PKG_FLOORS   := internal/workflow:88 cmd/connector-worker:55 internal/observability:90

.PHONY: metrics-lint metrics-inventory all tools tools-integration generate generate-sqlc mock contract-check contract-breaking \
        build migrate dev test test-integration test-e2e test-compat test-ci merge-coverage \
        cover cover-func cover-gaps cover-html cover-check cover-check-pkg \
        arch-lint lint lint-fix vuln \
        tidy fmt-check fix check \
        setup install-hooks \
        docker-up docker-down \
        docker-build docker-build-server docker-build-worker docker-build-connector-worker docker-lint docker-trivy docker-check pin-base-images \
        helm-lint lint-workflows \
        extract-schemas schema-validate schema-diff schema-register schema-prune \
        clean help

all: generate build


## tools: Install all dev tooling into .tools/
tools:
	@mkdir -p $(TOOLS_DIR)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install github.com/oasdiff/oasdiff@$(OASDIFF_VERSION)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install go.uber.org/mock/mockgen@$(MOCKGEN_VERSION)
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh \
		| sh -s -- -b $(PWD)/$(TOOLS_DIR) $(GOLANGCI_VERSION)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install github.com/fe3dback/go-arch-lint@$(GOARCHLINT_VERSION)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)
	@OS=$$(uname -s); ARCH=$$(uname -m); \
	if [ "$$OS" = "Darwin" ]; then \
	    if command -v brew >/dev/null 2>&1; then \
	        brew install hadolint >/dev/null && cp "$$(brew --prefix)/bin/hadolint" $(TOOLS_DIR)/hadolint; \
	    else \
	        echo "hadolint: brew not found on macOS — install manually: brew install hadolint"; exit 1; \
	    fi; \
	else \
	    curl -sSfLo $(TOOLS_DIR)/hadolint \
	      "https://github.com/hadolint/hadolint/releases/download/$(HADOLINT_VERSION)/hadolint-$$OS-$$ARCH" && \
	    chmod +x $(TOOLS_DIR)/hadolint; \
	fi
	@OS=$$(uname -s); ARCH=$$(uname -m); \
	if [ "$$OS" = "Darwin" ] && [ "$$ARCH" = "arm64" ]; then TRIVY_OS_ARCH="macOS-ARM64"; \
	elif [ "$$OS" = "Darwin" ]; then TRIVY_OS_ARCH="macOS-64bit"; \
	elif [ "$$ARCH" = "aarch64" ] || [ "$$ARCH" = "arm64" ]; then TRIVY_OS_ARCH="Linux-ARM64"; \
	else TRIVY_OS_ARCH="Linux-64bit"; fi; \
	curl -sSfLo /tmp/trivy.tar.gz \
	  "https://github.com/aquasecurity/trivy/releases/download/v$(TRIVY_VERSION)/trivy_$(TRIVY_VERSION)_$$TRIVY_OS_ARCH.tar.gz" && \
	tar -xzf /tmp/trivy.tar.gz -C $(TOOLS_DIR) trivy && rm /tmp/trivy.tar.gz
	@OS=$$(uname -s | tr '[:upper:]' '[:lower:]'); ARCH=$$(uname -m); \
	if [ "$$ARCH" = "aarch64" ]; then ARCH="arm64"; fi; \
	if [ "$$ARCH" = "x86_64" ]; then ARCH="amd64"; fi; \
	curl -sSfLo /tmp/kubeconform.tar.gz \
	  "https://github.com/yannh/kubeconform/releases/download/v$(KUBECONFORM_VERSION)/kubeconform-$$OS-$$ARCH.tar.gz" && \
	tar -xzf /tmp/kubeconform.tar.gz -C $(TOOLS_DIR) kubeconform && rm /tmp/kubeconform.tar.gz
	@echo "✓ tools installed to $(TOOLS_DIR)/"

## tools-integration: Pre-pull Docker images used by integration tests (testcontainers-go)
tools-integration:
	docker pull $(TESTCONTAINERS_POSTGRES_IMAGE)
	docker pull $(TESTCONTAINERS_LOCALSTACK_IMAGE)
	docker pull $(TESTCONTAINERS_VALKEY_IMAGE)
	docker pull $(TESTCONTAINERS_TEMPORAL_IMAGE)
	@echo "✓ Docker images ready for integration tests"


## generate: Run sqlc (queries → postgres/db/)
generate: generate-sqlc

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


## build: Compile the server, worker, and connector-worker binaries to bin/
build:
	@mkdir -p $(BIN_DIR)
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/server ./cmd/server
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/worker ./cmd/worker
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/connector-worker ./cmd/connector-worker
	@echo "✓ binaries: $(BIN_DIR)/server, $(BIN_DIR)/worker, $(BIN_DIR)/connector-worker"

## migrate: Apply DB schema migrations (outbox + domain) and exit. Run before the server/worker.
migrate:
	go run ./cmd/server migrate

## dev: Run the server locally (requires .env to be populated).
dev:
	go run ./cmd/server

## test: Run unit tests with race detector and coverage (internal + cmd + test/unit + test/workflow + test/contract)
test:
	@mkdir -p $(COVERAGE_DIR)
	go test -race -count=1 \
	    -coverpkg=$$(go list ./internal/... ./cmd/... | grep -v '$(COVER_EXCLUDE_PKG)' | tr '\n' ',' | sed 's/,$$//') \
	    -coverprofile=$(COVERAGE_DIR)/unit.out -covermode=atomic \
	    ./internal/... ./cmd/... ./test/unit/... ./test/workflow/... ./test/contract/...
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

## test-e2e: Run the real instantiate -> dispatch -> complete round trip against live Temporal + Postgres + HTTP, plus cmd/connector-worker's own real dispatch/completion round trip against live Valkey too (needs the Temporal image too — slower than test-integration, kept separate)
test-e2e:
	AWS_ACCESS_KEY_ID=test \
	AWS_SECRET_ACCESS_KEY=test \
	AWS_EC2_METADATA_DISABLED=true \
	TESTCONTAINERS_RYUK_DISABLED=true \
	go test -race -count=1 -tags e2e -timeout 5m ./test/e2e/... ./cmd/connector-worker/...

## test-compat: Run the Definition Service (DEFINITION_SERVICE_DIR, default ../definition_service), this repository's connector worker and the Workflow-Events Consumer (EVENT_CONSUMER_DIR, default ../event_consumer) as real binaries against this service's real API, worker and stores
test-compat:
	AWS_EC2_METADATA_DISABLED=true \
	TESTCONTAINERS_RYUK_DISABLED=true \
	DEFINITION_SERVICE_DIR="$(DEFINITION_SERVICE_DIR)" \
	EVENT_CONSUMER_DIR="$(EVENT_CONSUMER_DIR)" \
	go test -count=1 -tags compat -timeout 15m ./test/compat/...

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
	$(GOLANGCI) run --fix --build-tags=integration,e2e,compat ./cmd/... ./internal/... ./test/...
	@echo "✓ formatting and lint fixes applied"

## check: Verify formatting/lint (read-only), run vet, tests, and coverage gate — full local CI pass
check:
	@echo "==> gofmt"
	@$(MAKE) fmt-check
	@echo "==> lint"
	$(GOLANGCI) run --build-tags=integration,e2e,compat ./cmd/... ./internal/... ./test/...
	@echo "==> go vet"
	go vet ./cmd/... ./internal/...
	@echo "==> arch-lint"
	$(MAKE) arch-lint
	@echo "==> metrics-lint"
	$(MAKE) metrics-lint
	@echo "==> contract-breaking"
	$(MAKE) contract-breaking
	@echo "==> design-sync-check-test"
	$(MAKE) design-sync-check-test
	@echo "==> test-ci (unit + integration, merged coverage gate)"
	$(MAKE) cover-check
	@echo "✓ all checks passed"

## arch-lint: Enforce Clean Architecture import direction via go-arch-lint
arch-lint:
	$(GOARCHLINT) check --project-path .

## contract-check: Fail if a copied contract in api/contracts differs from its source (CONTRACT_SRC_DIR=<checkout> reads it locally)
contract-check:
	@scripts/contract-check.sh

## contract-breaking: Fail on a breaking change to api/internal-openapi.yaml against CONTRACT_BASE_REF (default origin/main)
contract-breaking:
	@OASDIFF=$(OASDIFF) scripts/contract-breaking.sh

## lint: Run golangci-lint (read-only; exits non-zero on violations)
lint:
	$(GOLANGCI) run --build-tags=integration,e2e,compat ./cmd/... ./internal/... ./test/...

## lint-fix: Run golangci-lint with auto-fix
lint-fix:
	$(GOLANGCI) run --fix --build-tags=integration,e2e,compat ./cmd/... ./internal/... ./test/...

## metrics-lint: Observability standard: collector naming, metricslint on a real scrape, alert and doc references, inventory drift
metrics-lint:
	@bash .github/scripts/check-metric-naming.sh
	@bash .github/scripts/metricslint.sh

## metrics-inventory: Regenerate docs/observability/metric-registry.md from the registry and a real scrape
metrics-inventory:
	@METRICS_INVENTORY_WRITE=1 bash .github/scripts/metricslint.sh

## vuln: Run govulncheck to detect known vulnerabilities in dependencies
vuln:
	$(GOVULNCHECK) ./...


IMAGE_TAG ?= local

## docker-build: Build all three container images (requires GO_PRIVATE_TOKEN in env)
docker-build: docker-build-server docker-build-worker docker-build-connector-worker

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

## docker-build-connector-worker: Build only the cmd/connector-worker image (--target connector-worker)
docker-build-connector-worker:
	docker buildx build \
	  --target connector-worker \
	  --secret id=go_private_token,env=GO_PRIVATE_TOKEN \
	  --build-arg BUILD_VERSION=$(IMAGE_TAG) \
	  --load \
	  -t execution-service-connector-worker:$(IMAGE_TAG) .

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

## lint-workflows: Lint GitHub Actions workflow YAML (shellcheck of run: steps included)
lint-workflows:
	$(ACTIONLINT)

## helm-lint: Lint the Helm chart and validate its rendered manifests against the Kubernetes API schema
HELM_LINT_VALUES := --set-string secret.secretValues.DATABASE_URL=placeholder \
	  --set-string secret.secretValues.TEMPORAL_HOST_PORT=placeholder \
	  --set-string secret.secretValues.INTERNAL_API_TOKEN=placeholder \
	  --set-string secret.secretValues.DEFINITION_SERVICE_TOKEN=placeholder \
	  --set-string secret.secretValues.OUTBOX_RELAY_DATABASE_URL=placeholder \
	  --set-string secret.secretValues.SNS_TOPIC_ARN=placeholder \
	  --set-string secret.secretValues.OPENBAO_TOKEN=placeholder \
	  --set-string secret.secretValues.VALKEY_ADDR=placeholder \
	  --set-string env.eligibilityBaseURL=http://eligibility.invalid \
	  --set-string env.tokenServiceBaseURL=http://token-service.invalid

# The render goes to a file first: piped straight into kubeconform, a failed
# render reached it as empty input and passed.
HELM_CHART ?= deploy/helm
helm-lint:
	$(HELM) lint $(HELM_CHART) $(HELM_LINT_VALUES)
	@mkdir -p $(TOOLS_DIR)
	$(HELM) template $(HELM_CHART) $(HELM_LINT_VALUES) > $(TOOLS_DIR)/helm-rendered.yaml
	@for t in serviceaccount secret; do \
		$(HELM) template $(HELM_CHART) $(HELM_LINT_VALUES) -s templates/$$t.yaml | grep -q '"helm.sh/hook-weight": "-10"' \
			|| { echo "helm-lint: templates/$$t.yaml is not a pre-install hook, so the migration job cannot start on a first install"; exit 1; }; \
	done
	$(KUBECONFORM) -strict -skip PrometheusRule,ServiceMonitor $(TOOLS_DIR)/helm-rendered.yaml
	@echo "✓ helm chart valid"

## pin-base-images: Resolve current digests for Dockerfile base images and pin them (writes .docker-digests)
pin-base-images:
	@GOLANG_DIGEST=$$(docker buildx imagetools inspect golang:1.26-alpine | awk '/^Digest:/{print $$2; exit}'); \
	DISTROLESS_DIGEST=$$(docker buildx imagetools inspect gcr.io/distroless/static-debian12:nonroot | awk '/^Digest:/{print $$2; exit}'); \
	sed -E -i.bak "s|^(FROM golang:1\.26-alpine)(@sha256:[0-9a-f]+)?( AS [A-Za-z0-9_-]+)?|\\1@$$GOLANG_DIGEST\\3|" Dockerfile; \
	sed -E -i.bak "s|^(FROM gcr\.io/distroless/static-debian12:nonroot)(@sha256:[0-9a-f]+)?( AS [A-Za-z0-9_-]+)?|\\1@$$DISTROLESS_DIGEST\\3|" Dockerfile; \
	rm -f Dockerfile.bak; \
	printf 'golang:1.26-alpine@%s\ngcr.io/distroless/static-debian12:nonroot@%s\n' "$$GOLANG_DIGEST" "$$DISTROLESS_DIGEST" > .docker-digests; \
	echo "Pinned base images — see .docker-digests"


# platform-schemagov — event-schema governance (extract/validate/diff/register/prune)
# against api/asyncapi.yaml + internal/eventschema/ (LLD §6.8). Structurally
# copied from definition_service's Makefile (iam-user-profile agrees on shape).
SCHEMA_GOV_IMAGE ?= ghcr.io/bcbp-solutions-fzc-llc/platform-schemagov:0.4

## extract-schemas: Derive internal/eventschema/*.json from api/asyncapi.yaml
extract-schemas:
	docker run --rm -v "$(CURDIR)":/workspace "$(SCHEMA_GOV_IMAGE)" extract \
	  --asyncapi   api/asyncapi.yaml \
	  --schema-dir internal/eventschema

## schema-validate: Structural/lifecycle/drift checks against api/asyncapi.yaml + internal/eventschema (no AWS required)
schema-validate: extract-schemas
	docker run --rm -v "$(CURDIR)":/workspace "$(SCHEMA_GOV_IMAGE)" validate \
	  --asyncapi   api/asyncapi.yaml \
	  --schema-dir internal/eventschema

## schema-diff: Diff two JSON Schema files — usage: make schema-diff CURRENT=<current.json> PROPOSED=<proposed.json> [SCHEMA_NAME=<name>]
schema-diff:
	@test -n "$(CURRENT)" && test -n "$(PROPOSED)" || { \
	  echo "Usage: make schema-diff CURRENT=<current.json> PROPOSED=<proposed.json> [SCHEMA_NAME=<name>]"; \
	  exit 1; \
	}
	docker run --rm -v "$(CURDIR)":/workspace "$(SCHEMA_GOV_IMAGE)" diff \
	  --current     "$(CURRENT)" \
	  --proposed    "$(PROPOSED)" \
	  --schema-name "$(or $(SCHEMA_NAME),$(notdir $(basename $(PROPOSED))))"

## schema-register: Register schemas in AWS Glue (requires AWS creds or LocalStack via AWS_ENDPOINT_URL)
schema-register:
	@test -n "$(GLUE_REGISTRY_NAME)" || { \
	  echo "GLUE_REGISTRY_NAME is not set — add it to .env or pass on the command line"; \
	  exit 1; \
	}
	docker run --rm -v "$(CURDIR)":/workspace \
	  -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_ENDPOINT_URL \
	  "$(SCHEMA_GOV_IMAGE)" register \
	  --registry   "$(GLUE_REGISTRY_NAME)" \
	  --schema-dir internal/eventschema

## schema-prune: Report orphaned Glue schemas (set EXECUTE=true to actually delete)
schema-prune:
	@test -n "$(GLUE_REGISTRY_NAME)" || { \
	  echo "GLUE_REGISTRY_NAME is not set — add it to .env or pass on the command line"; \
	  exit 1; \
	}
	docker run --rm -v "$(CURDIR)":/workspace \
	  -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_ENDPOINT_URL \
	  "$(SCHEMA_GOV_IMAGE)" prune \
	  --registry "$(GLUE_REGISTRY_NAME)" \
	  $(if $(filter true,$(EXECUTE)),--execute,)


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
	rm -rf internal/adapter/outbound/postgres/db/
	rm -rf internal/core/port/mocks/


## help: List available targets
help:
	@grep -E '^## ' Makefile | sed 's/## /  /'

.PHONY: design-sync-check design-sync-check-test

## design-sync-check: Fail if a document kept in the design repository too differs (DESIGN_DIR=<checkout> reads it locally, else GH_TOKEN reads Haroon-BCBP/design)
design-sync-check:
	@scripts/design-sync-check.sh

## design-sync-check-test: Test the design-sync-check script against throwaway directories
design-sync-check-test:
	@scripts/design-sync-check_test.sh
