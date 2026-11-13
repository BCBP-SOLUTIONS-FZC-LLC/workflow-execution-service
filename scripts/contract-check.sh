#!/usr/bin/env bash
# Checks each copied contract in api/contracts against its source file in the
# neighbouring service's repository. Each SOURCES line is:
#   <copy> <owner/repo> <path in that repo> <commit the copy was taken at> [<routes>]
# An identical copy passes. With OASDIFF set, a copy that differs fails only
# when the source has a breaking change (oasdiff breaking, copy to source) on
# a path matching <routes>, a regular expression of the routes this service
# calls (every path when absent); wording, examples and other routes pass.
# Without OASDIFF any difference fails.
# The ref read in the source repository is, in order: CONTRACT_SRC_REF; the
# branch of that repository named like the branch under test (HEAD_REF, else
# the current git branch) when it exists; main. No branch name is written down.
# CONTRACT_SRC_DIR reads a local checkout with git; otherwise the file is
# fetched with gh, which needs GH_TOKEN with read access to the source repo.
# A source file not on the ref yet is reported as pending and passes: nothing
# published can have drifted. A repo or ref that cannot be read fails.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
dir="$root/api/contracts"
if [[ -n "${CONTRACT_SRC_DIR:-}" ]] && ! git -C "$CONTRACT_SRC_DIR" rev-parse --git-dir >/dev/null 2>&1; then
  echo "✗ CONTRACT_SRC_DIR=$CONTRACT_SRC_DIR is not a git checkout" >&2
  exit 1
fi

head_branch="${HEAD_REF:-$(git -C "$root" branch --show-current 2>/dev/null || true)}"

branch_exists() {
  local repo="$1" name="$2"
  if [[ -n "${CONTRACT_SRC_DIR:-}" ]]; then
    git -C "$CONTRACT_SRC_DIR" rev-parse --verify --quiet "refs/heads/$name^{commit}" >/dev/null ||
      git -C "$CONTRACT_SRC_DIR" rev-parse --verify --quiet "refs/remotes/origin/$name^{commit}" >/dev/null
  else
    gh api "repos/$repo/git/ref/heads/$name" --silent 2>/dev/null
  fi
}

resolve_ref() {
  local repo="$1"
  if [[ -n "${CONTRACT_SRC_REF:-}" ]]; then
    printf '%s' "$CONTRACT_SRC_REF"
  elif [[ -n "$head_branch" && "$head_branch" != main ]] && branch_exists "$repo" "$head_branch"; then
    printf '%s' "$head_branch"
  else
    printf 'main'
  fi
}

if [[ -n "${OASDIFF:-}" && ! -x "$OASDIFF" ]]; then
  echo "✗ oasdiff not found at $OASDIFF (run make tools)" >&2
  exit 1
fi
source_tmp="$(mktemp)"
trap 'rm -f "$source_tmp"' EXIT

status=0
while read -r copy repo path commit routes; do
  [[ -z "${copy:-}" || "$copy" == \#* ]] && continue
  ref="$(resolve_ref "$repo")"
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
  if diff -q <(printf '%s\n' "$source_file") "$dir/$copy" >/dev/null; then
    echo "✓ $copy matches $repo:$path@$ref (copied at ${commit:0:7})"
    continue
  fi
  if [[ -z "${OASDIFF:-}" ]]; then
    diff -u <(printf '%s\n' "$source_file") "$dir/$copy" || true
    echo "✗ $copy differs from $repo:$path@$ref; copy it again, run the contract tests, and update SOURCES" >&2
    status=1
    continue
  fi
  printf '%s\n' "$source_file" >"$source_tmp"
  args=(breaking "$dir/$copy" "$source_tmp" --fail-on ERR)
  [[ -n "${routes:-}" ]] && args+=(--match-path "$routes")
  if report="$("$OASDIFF" "${args[@]}" 2>&1)"; then
    echo "✓ $copy differs from $repo:$path@$ref only in ways that cannot break the routes this service calls; refresh it when convenient"
  else
    printf '%s\n' "$report" >&2
    echo "✗ $copy: $repo:$path@$ref has a breaking change for the routes this service calls; copy it again, run the contract tests, and update SOURCES" >&2
    status=1
  fi
done < "$dir/SOURCES"
exit "$status"
