#!/usr/bin/env bash
# Tests scripts/design-sync-check.sh against throwaway repo and design directories.
set -uo pipefail

script="$(cd "$(dirname "$0")" && pwd)/design-sync-check.sh"
failures=0
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

fresh() {
  rm -rf "$work/repo" "$work/design"
  mkdir -p "$work/repo/scripts" "$work/design/LLD"
}

run() {
  out="$(DESIGN_SYNC_ROOT="$work/repo" DESIGN_SYNC_MANIFEST="$work/repo/scripts/design-sync.manifest" DESIGN_DIR="${1:-$work/design}" "$script" 2>&1)"
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
printf 'a: 1\n' >"$work/repo/spec.yaml"
printf 'a: 1\n' >"$work/design/LLD/spec.yaml"
printf 'raw spec.yaml LLD/spec.yaml\n' >"$work/repo/scripts/design-sync.manifest"
run
expect "equal raw files pass" 0 "✓ spec.yaml matches LLD/spec.yaml"

printf 'a: 2\n' >"$work/design/LLD/spec.yaml"
run
expect "differing raw files fail and show the difference" 1 "+a: 2"

fresh
printf '# Title\n\n> **Note:** This is the in-repo copy of this document.\n\n## One\n\ntext\n' >"$work/repo/doc.md"
printf '## One\n\ntext\n' >"$work/design/LLD/doc.md"
printf 'lld doc.md LLD/doc.md\n' >"$work/repo/scripts/design-sync.manifest"
run
expect "an LLD differing only by banner and title passes" 0 "✓ doc.md matches"

printf '# Title\n\n> **In-repo copy.** same words.\n\n## One\n\ntext\n' >"$work/repo/doc.md"
printf '# Title\n\n## One\n\ntext\n' >"$work/design/LLD/doc.md"
run
expect "the other banner wording passes too" 0

printf '# Title\n\n> **In-repo copy.** same words.\n\n## One\n\ntext changed\n' >"$work/repo/doc.md"
run
expect "an LLD with different content fails" 1 "differs from LLD/doc.md"

fresh
printf 'x\n' >"$work/repo/spec.yaml"
printf 'raw spec.yaml LLD/spec.yaml\n' >"$work/repo/scripts/design-sync.manifest"
run
expect "a design file that is not there fails" 1 "cannot read LLD/spec.yaml"

printf 'raw missing.yaml LLD/spec.yaml\n' >"$work/repo/scripts/design-sync.manifest"
printf 'x\n' >"$work/design/LLD/spec.yaml"
run
expect "a repo file that is not there fails" 1 "not in this repository"

fresh
mkdir -p "$work/repo/schemas" "$work/design/LLD/schemas"
printf '{}\n' >"$work/repo/schemas/a.json"
printf '{}\n' >"$work/design/LLD/schemas/a.json"
printf '{"x":1}\n' >"$work/repo/schemas/only-in-repo.json"
printf 'dir schemas LLD/schemas\n' >"$work/repo/scripts/design-sync.manifest"
run
expect "a directory pair passes and ignores a file only the repo has" 0 "✓ schemas/a.json matches"

printf '{"x":2}\n' >"$work/design/LLD/schemas/a.json"
run
expect "a differing file of a directory pair fails" 1 "schemas/a.json differs"

rm "$work/repo/schemas/a.json"
run
expect "a design file with no repo counterpart fails" 1 "schemas/a.json: not in this repository"

fresh
printf '# a comment\n\n' >"$work/repo/scripts/design-sync.manifest"
run
expect "comments and blank lines are ignored" 0

printf 'weird a b\n' >"$work/repo/scripts/design-sync.manifest"
run
expect "an unknown kind fails" 1 "unknown kind 'weird'"

printf 'raw only-one-path\n' >"$work/repo/scripts/design-sync.manifest"
run
expect "a short manifest line fails" 1 "needs <kind> <repo path> <design path>"

run "$work/not-a-directory"
expect "a DESIGN_DIR that is not a directory fails" 1 "is not a directory"

if [[ "$failures" -ne 0 ]]; then
  echo "$failures design-sync-check test(s) failed"
  exit 1
fi
echo "design-sync-check: all tests passed"
