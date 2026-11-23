#!/usr/bin/env bash
# Starts or stops the stack the black-box checks (make hurl, make fuzz-api, make fuzz-internal-api)
# run against: Execution and Definition infrastructure, the Definition stubs, the real Definition
# Service, a token service stub, and the Execution API and worker. The checks write data, so use
# throwaway databases (the compose ones are).
#
#   scripts/blackbox-stack.sh up     build and start everything, wait until it answers
#   scripts/blackbox-stack.sh down   stop everything
#
# DEFINITION_SERVICE_DIR names the Definition Service checkout (default ../definition_service).
# Ports: 8080 Execution API, 9091 its metrics, 8081 membership stub, 8082 Definition API,
# 8083 execution stub, 8084 catalog stub, 8085 token service stub, 8090 worker health,
# 9092 worker metrics, 9093 Definition metrics. Logs and binaries go to .coverage/blackbox.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$root/.coverage/blackbox"
def="${DEFINITION_SERVICE_DIR:-$root/../definition_service}"
pids="$work/pids"

wait_listening() {
  local name="$1" port="$2" log="$3"
  for _ in $(seq 1 90); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then return 0; fi
    sleep 1
  done
  echo "$name did not listen on :$port" >&2
  tail -n 30 "$log" >&2 || true
  return 1
}

wait_ok() {
  local name="$1" url="$2" log="$3"
  for _ in $(seq 1 90); do
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then return 0; fi
    sleep 1
  done
  echo "$name did not answer 200 on $url" >&2
  tail -n 30 "$log" >&2 || true
  return 1
}

# The Makefile exports the Execution Service's .env into its recipes. The Definition Service reads
# the same variable names, so its commands run without any variable the Execution .env defines.
as_definition() {
  local name
  for name in $(grep -oE '^[A-Z][A-Z0-9_]*' "$root/.env.example" "$root/.env" | cut -d: -f2 | sort -u); do
    unset "$name"
  done
  "$@"
}

start() {
  local name="$1"
  shift
  "$@" >"$work/$name.log" 2>&1 &
  echo $! >>"$pids"
}

up() {
  mkdir -p "$work/bin"
  : >"$pids"
  [ -f "$root/.env" ] || cp "$root/.env.example" "$root/.env"
  [ -f "$def/.env" ] || cp "$def/.env.example" "$def/.env"

  (cd "$root" && docker compose up -d --wait --wait-timeout 180)
  (cd "$def" && docker compose up -d --wait --wait-timeout 180)
  (cd "$root" && go run ./cmd/server migrate >"$work/migrate-execution.log" 2>&1)
  (cd "$def" && as_definition go run ./cmd/server migrate >"$work/migrate-definition.log" 2>&1)

  for stub in membership catalog execution; do
    (cd "$def" && go build -o "$work/bin/stub-$stub" "./cmd/stub/$stub")
  done
  (cd "$def" && go build -o "$work/bin/definition-server" ./cmd/server)
  (cd "$root" && go build -o "$work/bin/token-stub" ./test/hurl/stub/tokenservice)
  (cd "$root" && go build -o "$work/bin/execution-server" ./cmd/server)
  (cd "$root" && go build -o "$work/bin/execution-worker" ./cmd/worker)

  for stub in membership catalog execution; do
    (cd "$def" && as_definition start "stub-$stub" "$work/bin/stub-$stub")
  done
  (cd "$def" && as_definition start definition env HTTP_PORT=8082 METRICS_PORT=9093 \
    ORG_MEMBERSHIP_BASE_URL=http://localhost:8081 CATALOG_ADMIN_BASE_URL=http://localhost:8084 \
    EXECUTION_SERVICE_URL=http://localhost:8083 "$work/bin/definition-server")
  start token-stub "$work/bin/token-stub"
  local execution_env=(APP_ENV=dev AWS_USE_STUB=true ELIGIBILITY_BASE_URL=http://localhost:8081
    TOKEN_SERVICE_BASE_URL=http://localhost:8085 DEFINITION_SERVICE_URL=http://localhost:8082)
  (cd "$root" && start execution-server env "${execution_env[@]}" "$work/bin/execution-server")
  (cd "$root" && start execution-worker env "${execution_env[@]}" \
    WORKER_HEALTH_PORT=8090 WORKER_METRICS_PORT=9092 "$work/bin/execution-worker")

  wait_listening "membership stub" 8081 "$work/stub-membership.log"
  wait_listening "execution stub" 8083 "$work/stub-execution.log"
  wait_listening "catalog stub" 8084 "$work/stub-catalog.log"
  wait_ok "token service stub" http://localhost:8085/healthz "$work/token-stub.log"
  wait_ok "Definition Service" http://localhost:8082/readyz "$work/definition.log"
  wait_ok "Execution API" http://localhost:8080/readyz "$work/execution-server.log"
  wait_ok "Execution worker" http://localhost:8090/healthz "$work/execution-worker.log"
  echo "black-box stack is up"
}

down() {
  if [ -f "$pids" ]; then
    while read -r pid; do kill "$pid" 2>/dev/null || true; done <"$pids"
    : >"$pids"
  fi
  (cd "$root" && docker compose stop >/dev/null 2>&1) || true
  (cd "$def" && docker compose stop >/dev/null 2>&1) || true
}

case "${1:-}" in
up) up ;;
down) down ;;
*)
  echo "usage: $0 up|down" >&2
  exit 2
  ;;
esac
