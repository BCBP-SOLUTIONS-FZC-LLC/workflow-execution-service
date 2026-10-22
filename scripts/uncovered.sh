#!/usr/bin/env bash
# Usage: ./scripts/uncovered.sh [profile-path]
# Defaults to .coverage/coverage.out (produced by 'make test-ci').
set -euo pipefail

PROFILE="${1:-.coverage/coverage.out}"
MODULE="github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/"
EXCLUDE="postgres/db/|/mocks/"

[ -f "$PROFILE" ] || { echo "profile not found: $PROFILE  (run 'make test-ci' first)" >&2; exit 1; }

strip() { sed "s|${MODULE}||g"; }

funcs=$(go tool cover -func="$PROFILE" | grep -vE "($EXCLUDE)" | grep -v "^total:")

zero=$(echo "$funcs"  | awk '$NF=="0.0%"')
part=$(echo "$funcs"  | awk '$NF!="0.0%" && $NF!="100.0%"')
full=$(echo "$funcs"  | awk '$NF=="100.0%"' | wc -l | tr -d ' ')
total=$(echo "$funcs" | wc -l | tr -d ' ')

printf '\n== Uncovered functions (0.0%%) ==\n'
if [ -n "$zero" ]; then
  echo "$zero" | strip | awk '{printf "  %-65s %s\n", $1, $2}' | sort
else
  echo "  (none)"
fi

printf '\n== Partially covered functions (<100%%) ==\n'
if [ -n "$part" ]; then
  echo "$part" | strip | awk '{printf "  %-65s %s\n", $1, $3}' | sort
else
  echo "  (none)"
fi

printf '\n== Summary ==\n'
printf '  Profile             : %s\n' "$PROFILE"
printf '  Uncovered (0.0%%)    : %d\n'  "$(echo "$zero" | grep -c . || true)"
printf '  Partial (<100%%)     : %d\n'  "$(echo "$part" | grep -c . || true)"
printf '  Fully covered       : %s\n'  "$full"
printf '  Total functions     : %s\n'  "$total"
printf '\n'
