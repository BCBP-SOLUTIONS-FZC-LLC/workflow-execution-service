#!/usr/bin/env bash
# Fails when a document kept both in this repository and in the design repository differs.
# Each line of scripts/design-sync.manifest is:  <kind> <repo path> <design path>
#   lld  Markdown, compared after dropping the in-repo banner lines and the first H1 title
#   raw  compared byte for byte
#   dir  every file of the design directory must equal the same-named file of the repo directory
# DESIGN_DIR reads a local checkout of the design repository. Without it the file is fetched with
# gh from DESIGN_REPO (default Haroon-BCBP/design) at DESIGN_REF (default main), which needs GH_TOKEN
# with read access. A design file that cannot be read, or is not there, fails the check.
set -euo pipefail

root="${DESIGN_SYNC_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
manifest="${DESIGN_SYNC_MANIFEST:-$root/scripts/design-sync.manifest}"
design_repo="${DESIGN_REPO:-Haroon-BCBP/design}"
design_ref="${DESIGN_REF:-main}"

if [[ -n "${DESIGN_DIR:-}" && ! -d "$DESIGN_DIR" ]]; then
  echo "✗ DESIGN_DIR=$DESIGN_DIR is not a directory" >&2
  exit 1
fi

normalise_lld() {
  awk '/^> .*[Ii]n-repo copy/ { next } NR <= 8 && /^# / && !title { title = 1; next } { print }' | sed '/./,$!d'
}

design_file() {
  local path="$1"
  if [[ -n "${DESIGN_DIR:-}" ]]; then
    [[ -f "$DESIGN_DIR/$path" ]] && cat "$DESIGN_DIR/$path"
  else
    gh api -H 'Accept: application/vnd.github.raw' "repos/$design_repo/contents/$path?ref=$design_ref"
  fi
}

design_files_in() {
  local path="$1"
  if [[ -n "${DESIGN_DIR:-}" ]]; then
    [[ -d "$DESIGN_DIR/$path" ]] && (cd "$DESIGN_DIR/$path" && find . -maxdepth 1 -type f | sed 's|^\./||' | sort)
  else
    gh api "repos/$design_repo/contents/$path?ref=$design_ref" --jq '.[] | select(.type == "file") | .name'
  fi
}

status=0

compare() {
  local kind="$1" repo_path="$2" design_path="$3" repo_content design_content
  if [[ ! -f "$root/$repo_path" ]]; then
    echo "✗ $repo_path: not in this repository" >&2
    status=1
    return
  fi
  if ! design_content="$(design_file "$design_path")"; then
    echo "✗ $repo_path: cannot read $design_path from the design repository" >&2
    status=1
    return
  fi
  repo_content="$(cat "$root/$repo_path")"
  if [[ "$kind" == lld ]]; then
    repo_content="$(printf '%s\n' "$repo_content" | normalise_lld)"
    design_content="$(printf '%s\n' "$design_content" | normalise_lld)"
  fi
  if diff -u --label "repo:$repo_path" --label "design:$design_path" <(printf '%s\n' "$repo_content") <(printf '%s\n' "$design_content") >&2; then
    echo "✓ $repo_path matches $design_path"
  else
    echo "✗ $repo_path differs from $design_path; copy the newer one over the older, in both repositories' history" >&2
    status=1
  fi
}

compare_dir() {
  local repo_dir="$1" design_dir="$2" names name
  if ! names="$(design_files_in "$design_dir")"; then
    echo "✗ $repo_dir: cannot list $design_dir in the design repository" >&2
    status=1
    return
  fi
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    compare raw "$repo_dir/$name" "$design_dir/$name"
  done <<<"$names"
}

while read -r kind repo_path design_path extra; do
  [[ -z "${kind:-}" || "$kind" == \#* ]] && continue
  if [[ -z "${repo_path:-}" || -z "${design_path:-}" || -n "${extra:-}" ]]; then
    echo "✗ manifest line needs <kind> <repo path> <design path>: $kind $repo_path $design_path $extra" >&2
    status=1
    continue
  fi
  case "$kind" in
    lld | raw) compare "$kind" "$repo_path" "$design_path" ;;
    dir) compare_dir "$repo_path" "$design_path" ;;
    *)
      echo "✗ unknown kind '$kind' in the manifest" >&2
      status=1
      ;;
  esac
done <"$manifest"
exit "$status"
