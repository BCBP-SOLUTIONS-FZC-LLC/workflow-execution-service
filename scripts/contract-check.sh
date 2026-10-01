#!/usr/bin/env bash
# Fails when a copied contract in api/contracts differs from the file it was
# copied from, at the commit api/contracts/SOURCES names. Each SOURCES line is:
#   <copy> <owner/repo> <path in that repo> <commit>
# With CONTRACT_SRC_DIR set to a local checkout of the source repo, the file is
# read from it with git; otherwise it is fetched with gh, which needs a token
# with read access to the source repo.
set -euo pipefail

dir="$(cd "$(dirname "$0")/.." && pwd)/api/contracts"
status=0
while read -r copy repo path commit; do
  [[ -z "${copy:-}" || "$copy" == \#* ]] && continue
  if [[ -n "${CONTRACT_SRC_DIR:-}" ]]; then
    source_file="$(git -C "$CONTRACT_SRC_DIR" show "$commit:$path")"
  else
    source_file="$(gh api -H 'Accept: application/vnd.github.raw' "repos/$repo/contents/$path?ref=$commit")"
  fi
  if diff -u <(printf '%s\n' "$source_file") "$dir/$copy" >/dev/null; then
    echo "✓ $copy matches $repo:$path@${commit:0:7}"
  else
    echo "✗ $copy differs from $repo:$path@${commit:0:7}; copy that file again and update SOURCES" >&2
    status=1
  fi
done < "$dir/SOURCES"
exit "$status"
