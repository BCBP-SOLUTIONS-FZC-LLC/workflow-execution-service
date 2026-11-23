#!/usr/bin/env bash
# Local black-box checks for the execution service: this file describes the stack, `make hurl` runs it.
#
# Nothing here starts a process. Start each line below in its own terminal, in this order, then run
# `make hurl` (and `make fuzz-api` / `make fuzz-internal-api`). The checks write data into the
# databases below, so use throwaway ones; none of this belongs in CI.
#
# Ports used by the recipe (the defaults would collide, so some are moved):
#   8080 execution API      9091 execution metrics     8081 membership stub   8082 definition API
#   8083 execution stub     8084 catalog stub          8085 token service stub
#   8090 execution worker health   9092 execution worker metrics   9093 definition metrics
#
# 1. Execution infrastructure (Postgres :5433, Valkey :6380, LocalStack, PgBouncer, Temporal :7233):
#      make docker-up
#
# 2. Execution schema, as the compose database owner given by DATABASE_URL in .env.example:
#      make migrate
#
# 3. Definition Service infrastructure and schema (its compose uses Postgres :5432, Valkey :6379):
#      (cd ../definition_service && make docker-up && make migrate)
#
# 4. Definition Service stubs (cmd/stub/membership accepts every member as eligible):
#      (cd ../definition_service && go run ./cmd/stub/membership)   # :8081
#      (cd ../definition_service && go run ./cmd/stub/catalog)      # :8084
#      (cd ../definition_service && go run ./cmd/stub/execution)    # :8083
#
# 5. The real Definition Service binary, moved off the execution API's ports:
#      cd ../definition_service && \
#      HTTP_PORT=8082 METRICS_PORT=9093 \
#      ORG_MEMBERSHIP_BASE_URL=http://localhost:8081 \
#      CATALOG_ADMIN_BASE_URL=http://localhost:8084 \
#      EXECUTION_SERVICE_URL=http://localhost:8083 \
#      go run ./cmd/server
#
# 6. The token service stub (answers every tenant's platform-automation account as active):
#      go run ./test/hurl/stub/tokenservice                          # :8085, STUB_ADDR to change
#
# 7. The execution API and worker; both read .env, and these variables take precedence over it:
#      export APP_ENV=dev AWS_USE_STUB=true \
#             ELIGIBILITY_BASE_URL=http://localhost:8081 \
#             TOKEN_SERVICE_BASE_URL=http://localhost:8085 \
#             DEFINITION_SERVICE_URL=http://localhost:8082
#      go run ./cmd/server
#      WORKER_HEALTH_PORT=8090 WORKER_METRICS_PORT=9092 go run ./cmd/worker
#
# Targets read API_BASE, METRICS_BASE, DEFINITION_BASE, SCENARIO_TENANT and SCENARIO_USER from the
# environment when the defaults above do not fit.

echo "scripts/smoke-test.sh only describes the stack: read its comments, start the stack, run 'make hurl'" >&2
exit 1
