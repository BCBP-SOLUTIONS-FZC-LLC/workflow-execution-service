#!/usr/bin/env bash
# Tests scripts/contract-check.sh against throwaway source and service repositories.
set -uo pipefail

script="$(cd "$(dirname "$0")" && pwd)/contract-check.sh"
failures=0
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

git_quiet() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main "$@" >/dev/null 2>&1; }

fresh() {
  rm -rf "$work/src" "$work/svc"
  mkdir -p "$work/src" "$work/svc/scripts" "$work/svc/api/contracts"
  cp "$script" "$work/svc/scripts/contract-check.sh"
  git_quiet -C "$work/src" init
  printf 'v: main\n' >"$work/src/spec.yaml"
  git_quiet -C "$work/src" add spec.yaml
  git_quiet -C "$work/src" commit -m main
  git_quiet -C "$work/src" switch -c feat/x
  printf 'v: branch\n' >"$work/src/spec.yaml"
  git_quiet -C "$work/src" commit -am branch
  git_quiet -C "$work/src" switch main
  printf 'copy.yaml acme/src spec.yaml 0123456789\n' >"$work/svc/api/contracts/SOURCES"
  git_quiet -C "$work/svc" init
  git -C "$work/svc" symbolic-ref HEAD "refs/heads/${1:-feat/other}"
}

run() {
  out="$(env -u HEAD_REF -u CONTRACT_SRC_REF CONTRACT_SRC_DIR="$work/src" "$@" "$work/svc/scripts/contract-check.sh" 2>&1)"
  code=$?
}

expect() {
  local name="$1" want_code="$2" want_text="${3:-}"
  if [[ "$code" != "$want_code" ]] || { [[ -n "$want_text" ]] && ! grep -qF -- "$want_text" <<<"$out"; }; then
    echo "FAIL: $name (exit $code, wanted $want_code${want_text:+ and \"$want_text\"})"
    echo "$out" | sed 's/^/    /'
    failures=$((failures + 1))
  else
    echo "ok:   $name"
  fi
}

fresh
printf 'v: main\n' >"$work/svc/api/contracts/copy.yaml"
run env
expect "a branch with no same-named source branch reads main" 0 "@main"

fresh
printf 'v: branch\n' >"$work/svc/api/contracts/copy.yaml"
run env
expect "a copy of the other branch fails against main" 1 "differs from"

fresh feat/x
printf 'v: branch\n' >"$work/svc/api/contracts/copy.yaml"
run env
expect "the current git branch picks the same-named source branch" 0 "@feat/x"

fresh
printf 'v: branch\n' >"$work/svc/api/contracts/copy.yaml"
run env HEAD_REF=feat/x
expect "HEAD_REF picks the same-named source branch" 0 "@feat/x"

fresh feat/x
printf 'v: main\n' >"$work/svc/api/contracts/copy.yaml"
run env
expect "the same-named branch wins over main, so a copy of main fails" 1 "differs from"

fresh feat/x
printf 'v: main\n' >"$work/svc/api/contracts/copy.yaml"
run env CONTRACT_SRC_REF=main
expect "CONTRACT_SRC_REF overrides the same-named branch" 0 "@main"

fresh main
printf 'v: main\n' >"$work/svc/api/contracts/copy.yaml"
run env
expect "main under test reads main" 0 "@main"

fresh
printf 'v: main\n' >"$work/svc/api/contracts/copy.yaml"
run env CONTRACT_SRC_REF=nope
expect "an unknown override ref fails" 1 "is not a ref"

fresh
printf 'copy.yaml acme/src missing.yaml 0123456789\n' >"$work/svc/api/contracts/SOURCES"
printf 'x\n' >"$work/svc/api/contracts/copy.yaml"
run env
expect "a source file not on the ref yet is pending and passes" 0 "pending"

fresh
printf '# note\n\n' >"$work/svc/api/contracts/SOURCES"
run env
expect "comments and blank lines are skipped" 0

if [[ "$failures" -gt 0 ]]; then
  echo "$failures contract-check test(s) failed"
  exit 1
fi
echo "contract-check tests passed"
