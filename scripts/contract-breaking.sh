#!/usr/bin/env bash
# Fails when api/internal-openapi.yaml makes a breaking change against its
# version on CONTRACT_BASE_REF (default origin/main), the gate
# platform-schemagov's diff runs for event schemas. A spec not on the base
# ref yet passes as new. OASDIFF overrides the oasdiff binary.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
spec="api/internal-openapi.yaml"
base_ref="${CONTRACT_BASE_REF:-origin/main}"
oasdiff="${OASDIFF:-$root/.tools/oasdiff}"

base="$(mktemp)"
trap 'rm -f "$base"' EXIT
if ! git -C "$root" show "$base_ref:$spec" >"$base" 2>/dev/null; then
  echo "… $spec is not on $base_ref yet: a new contract, nothing to break"
  exit 0
fi
"$oasdiff" breaking "$base" "$root/$spec" --fail-on ERR
echo "✓ no breaking change to $spec against $base_ref"
