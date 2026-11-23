#!/usr/bin/env bash
# Runs the "Check schema freeze" step of the schema registration workflow, both jobs, under bash -e.
set -uo pipefail

workflow="$(cd "$(dirname "$0")/.." && pwd)/.github/workflows/schema-registry.yml"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
failures=0

python3 - "$workflow" "$work" <<'PY'
import re, sys

lines = open(sys.argv[1]).read().split("\n")
found = 0
for i, line in enumerate(lines):
    if line.strip() != "- name: Check schema freeze":
        continue
    j = next(k for k in range(i + 1, len(lines)) if lines[k].strip() == "run: |")
    indent = len(lines[j]) - len(lines[j].lstrip()) + 2
    body = []
    for k in range(j + 1, len(lines)):
        if lines[k].strip() and len(lines[k]) - len(lines[k].lstrip()) < indent:
            break
        body.append(lines[k][indent:] if lines[k].strip() else "")
    found += 1
    open(f"{sys.argv[2]}/step{found}.sh", "w").write("\n".join(body) + "\n")
if found != 2:
    sys.exit(f"expected 2 freeze steps, found {found}")
PY
[ $? -eq 0 ] || exit 1

expect() {
  local name="$1" want="$2" freeze="$3" force="$4" step="$5" out status
  out="$(SCHEMA_FREEZE="$freeze" FORCE_REGISTRATION="$force" ENVIRONMENT=staging bash -e "$work/$step" 2>&1)"
  status=$?
  if [ "$status" -ne "$want" ]; then
    echo "FAIL: $name ($step): exit $status, want $want: $out"
    failures=$((failures + 1))
  fi
}

for step in step1.sh step2.sh; do
  expect "no freeze and no force passes" 0 "" "" "$step"
  expect "no freeze and force false passes" 0 "" "false" "$step"
  expect "freeze set to false passes" 0 "false" "" "$step"
  expect "freeze without force is refused" 1 "true" "" "$step"
  expect "freeze with force false is refused" 1 "true" "false" "$step"
  expect "freeze with force true proceeds" 0 "true" "true" "$step"
  expect "force true with no freeze proceeds" 0 "" "true" "$step"
done

if [ "$failures" -ne 0 ]; then
  echo "$failures case(s) failed"
  exit 1
fi
echo "ok: schema freeze step"
