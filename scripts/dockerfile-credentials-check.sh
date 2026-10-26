#!/usr/bin/env bash
# Fails when a Dockerfile RUN that writes ~/.git-credentials does not remove it in the same
# RUN, or when a workflow reads or exports a registry build cache: both leave the private-module token
# in a layer that anyone who can read the cache can recover.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
dockerfile="${DOCKERFILE:-$root/Dockerfile}"
workflows="${WORKFLOWS_DIR:-$root/.github/workflows}"
status=0

while IFS= read -r instruction; do
  if [[ "$instruction" == *".git-credentials"* && "$instruction" != *"rm -f"*".git-credentials"* ]]; then
    echo "✗ $(basename "$dockerfile"): a RUN writes .git-credentials and does not remove it in the same RUN" >&2
    status=1
  fi
done < <(awk '/^RUN /{if (line) print line; line=$0; next} line && /^[[:space:]]/{line=line $0; next} {if (line) print line; line=""} END{if (line) print line}' \
  <(sed -e ':a' -e '/\\$/N; s/\\\n//; ta' "$dockerfile"))

if [[ -d "$workflows" ]] && grep -rnE 'type=registry,ref=.*buildcache' "$workflows" >&2; then
  echo "✗ a workflow uses a registry build cache; builder layers would carry the token" >&2
  status=1
fi

[[ $status -eq 0 ]] && echo "✓ no credential left in a Dockerfile layer or registry cache"
exit $status
