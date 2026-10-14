#!/usr/bin/env bash
# Fails when a copied contract in api/contracts differs from its source file
# on the branch api/contracts/SOURCES names. Each SOURCES line is:
#   <copy> <owner/repo> <path in that repo> <branch> <commit the copy was taken at>
# CONTRACT_SRC_REF reads another ref instead of the branch. CONTRACT_SRC_DIR
# reads a local checkout with git; otherwise the file is fetched with gh,
# which needs GH_TOKEN with read access to the source repo. A source file not
# on the ref yet is reported as pending and passes: nothing published can
# have drifted. A repo or ref that cannot be read fails.
set -euo pipefail

dir="$(cd "$(dirname "$0")/.." && pwd)/api/contracts"
if [[ -n "${CONTRACT_SRC_DIR:-}" ]] && ! git -C "$CONTRACT_SRC_DIR" rev-parse --git-dir >/dev/null 2>&1; then
  echo "✗ CONTRACT_SRC_DIR=$CONTRACT_SRC_DIR is not a git checkout" >&2
  exit 1
fi
status=0
while read -r copy repo path branch commit; do
  [[ -z "${copy:-}" || "$copy" == \#* ]] && continue
  ref="${CONTRACT_SRC_REF:-$branch}"
  if [[ -n "${CONTRACT_SRC_DIR:-}" ]]; then
    if ! git -C "$CONTRACT_SRC_DIR" rev-parse --verify --quiet "$ref^{commit}" >/dev/null; then
      echo "✗ $copy: $ref is not a ref in $CONTRACT_SRC_DIR" >&2
      status=1
      continue
    fi
    if ! source_file="$(git -C "$CONTRACT_SRC_DIR" show "$ref:$path" 2>/dev/null)"; then
      echo "… $copy: $path is not on $repo@$ref yet (pending)"
      continue
    fi
  elif ! gh api "repos/$repo/commits/$ref" --silent 2>/dev/null; then
    echo "✗ $copy: cannot read $repo@$ref (check the repo, the ref and GH_TOKEN)" >&2
    status=1
    continue
  elif ! source_file="$(gh api -H 'Accept: application/vnd.github.raw' "repos/$repo/contents/$path?ref=$ref" 2>/dev/null)"; then
    if grep -q 'HTTP 404' <<<"$(gh api "repos/$repo/contents/$path?ref=$ref" 2>&1 || true)"; then
      echo "… $copy: $path is not on $repo@$ref yet (pending)"
      continue
    fi
    echo "✗ $copy: cannot read $repo:$path@$ref (check GH_TOKEN)" >&2
    status=1
    continue
  fi
  if diff -u <(printf '%s\n' "$source_file") "$dir/$copy"; then
    echo "✓ $copy matches $repo:$path@$ref (copied at ${commit:0:7})"
  else
    echo "✗ $copy differs from $repo:$path@$ref; copy it again, run the contract tests, and update SOURCES" >&2
    status=1
  fi
done < "$dir/SOURCES"
exit "$status"
