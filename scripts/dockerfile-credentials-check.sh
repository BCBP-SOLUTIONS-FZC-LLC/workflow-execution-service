#!/usr/bin/env bash
# Fails when the private-module token can stay in an image layer or a build cache:
#   - a Dockerfile RUN that reads a build secret or writes git credentials must remove them in
#     that RUN with `trap '... rm -f ~/.git-credentials ...' EXIT`, which runs wherever the RUN ends;
#   - a RUN written as a heredoc is refused, since its body is not scanned;
#   - a workflow must not read or export a registry build cache, which carries builder layers.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
dockerfile="${DOCKERFILE:-$root/Dockerfile}"
workflows="${WORKFLOWS_DIR:-$root/.github/workflows}"
status=0

instructions="$(grep -v '^[[:space:]]*#' "$dockerfile" | sed -e ':a' -e '/\\$/N; s/\\\n//; ta')"

while IFS= read -r instruction; do
  [[ "$instruction" == RUN\ * ]] || continue
  if [[ "$instruction" =~ ^RUN[[:space:]]+(--[^[:space:]]+[[:space:]]+)*\<\< ]]; then
    echo "✗ $(basename "$dockerfile"): a heredoc RUN is not scanned; write the RUN on one logical line" >&2
    status=1
    continue
  fi
  touches_credentials=0
  case "$instruction" in
    *"/run/secrets/"* | *".git-credentials"* | *".netrc"* | *"insteadOf"*) touches_credentials=1 ;;
  esac
  if [[ $touches_credentials -eq 1 ]] && ! [[ "$instruction" =~ trap\ \'[^\']*rm\ -f[^\']*\.git-credentials[^\']*\'\ EXIT ]]; then
    echo "✗ $(basename "$dockerfile"): a RUN uses a build secret or writes credentials and does not end with trap '... rm -f ~/.git-credentials ...' EXIT" >&2
    status=1
  fi
done <<<"$instructions"

if [[ -d "$workflows" ]] && grep -rn 'type=registry' "$workflows" >&2; then
  echo "✗ a workflow uses a registry build cache; builder layers would carry the token" >&2
  status=1
fi

[[ $status -eq 0 ]] && echo "✓ no credential left in a Dockerfile layer or registry cache"
exit $status
