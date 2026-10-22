#!/usr/bin/env bash
# Enforces the Enterprise Platform Observability Standard's construction,
# naming and label vocabulary rules. Two scopes:
#
#   1. Codebase-wide: no non-test Go file outside internal/adapter/outbound/
#      metrics/ may build a collector with prometheus.New* directly. Every
#      collector must come from a gincommon constructor, which injects the
#      {domain, service, environment} const labels and validates the name.
#
#   2. Metrics package (internal/adapter/outbound/metrics/):
#      - no prometheus.New* (same rule — belt-and-suspenders in the one place
#        where the temptation is highest)
#      - NewShared*Vec builds only registry names (platform_* or
#        domain-shared workflow_*)
#      - registerServiceMetrics() builds only Tier 3 workflow_execution_service_* names
#        with gincommon.NewCounterVec / NewCounter / NewGaugeVec / NewGauge /
#        NewHistogramVec: snake_case, _total on counters, _seconds on
#        histograms, no _total on gauges
#      - label names on Tier 3 metrics are snake_case and do not use
#        prohibited high-cardinality patterns (*_id, email, *_ip)
#
# Does NOT validate the tier-classification judgment itself (shared vs.
# service-specific) — that's a review-time call per the standard's decision
# tree. Dynamic label vocabulary compliance and registry conformance are
# validated by platform-gincommon's metricslint (.github/scripts/metricslint.sh).
set -euo pipefail

DIR="internal/observability"
test -d "$DIR" || {
  echo "::error file=${DIR}::observability package not found"
  exit 1
}

python3 - "$DIR" <<'PYEOF'
import glob
import os
import re
import sys

pkg = sys.argv[1]
errors = []

# ── 1. Codebase-wide: no prometheus.New* outside the metrics package ──────────
# The metrics package is the single location where Prometheus collectors are
# built. Any other file using prometheus.New* directly creates a collector
# without the centrally-injected {domain, service, environment} const labels,
# violating the standard's label governance rules.
SKIP_DIRS = {"vendor", ".git", "bin", "docs", "deploy", "scripts", ".githooks"}
NEW_PATTERN = re.compile(
    r"prometheus\.New(Counter|CounterVec|Gauge|GaugeVec|Histogram|HistogramVec|Summary|SummaryVec)\("
)
for root, dirs, files in os.walk("."):
    dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
    for fname in files:
        if not fname.endswith(".go") or fname.endswith("_test.go"):
            continue
        fpath = os.path.normpath(os.path.join(root, fname))
        if fpath.startswith(pkg):
            continue  # checked in section 2
        try:
            text = open(fpath, encoding="utf-8").read()
        except OSError:
            continue
        for m in NEW_PATTERN.finditer(text):
            line = text[: m.start()].count("\n") + 1
            errors.append(
                f"{fpath}:{line}: prometheus.New{m.group(1)} outside the metrics package"
                f" — build all collectors in {pkg}/ with gincommon constructors so"
                f" the {{domain, service, environment}} identity labels are injected centrally"
            )

# ── 2. Metrics-package checks ─────────────────────────────────────────────────
sources = {
    p: open(p, encoding="utf-8").read()
    for p in sorted(glob.glob(f"{pkg}/*.go"))
    if not p.endswith("_test.go")
}

for path, text in sources.items():
    for m in NEW_PATTERN.finditer(text):
        line = text[: m.start()].count("\n") + 1
        errors.append(
            f"{path}:{line}: prometheus.New{m.group(1)}"
            f" — build collectors with gincommon's constructors"
            f" (NewShared*Vec for registry metrics, NewCounterVec/NewGauge/NewHistogramVec for Tier 3)"
        )

business = sources.get(f"{pkg}/metrics.go", "")

def body(name):
    m = re.search(rf"func {name}\(\)[^{{]*\{{\n(.*?)\n\}}\n", business, re.DOTALL)
    if not m:
        errors.append(f"{pkg}/metrics.go: could not locate {name}() body")
        return ""
    return m.group(1)

count = 0
for name in re.findall(r'gincommon\.NewShared\w+\(nil, "([^"]+)"\)', business):
    count += 1
    if not (name.startswith("platform_") or name.startswith("workflow_") and not name.startswith("workflow_execution_service_")):
        errors.append(f'"{name}": NewShared*Vec builds registry metrics only (platform_* or domain-shared workflow_*)')

# Prohibited label name patterns (Enterprise Platform Observability Standard,
# §"High-cardinality labels are prohibited"). These are caught at runtime by
# gincommon and dynamically by metricslint; this static check surfaces them
# earlier, at lint time, with a pointer to the source line.
PROHIBITED_LABELS = [
    (re.compile(r".*_id$"), "ends in _id — high-cardinality identifier (user_id, tenant_id, event_id, …)"),
    (re.compile(r"^email$"), "email is a prohibited high-cardinality label"),
    (re.compile(r"^ip$|.*_ip$"), "IP address label (*_ip or ip) is prohibited"),
]

def check_label_names(metric_name, labels):
    for label in labels:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", label):
            errors.append(
                f'"{metric_name}": label "{label}" is not snake_case'
                f" (must match [a-z][a-z0-9_]*)"
            )
        for pattern, reason in PROHIBITED_LABELS:
            if pattern.match(label):
                errors.append(f'"{metric_name}": label "{label}" — {reason}')

tier3_body = body("registerServiceMetrics")
tier3 = re.findall(
    r'gincommon\.New(CounterVec|Counter|GaugeVec|Gauge|HistogramVec)\(nil,\s*"([^"]+)"',
    tier3_body,
)
if not tier3:
    errors.append(f"{pkg}/metrics.go: registerServiceMetrics() builds no collectors")
for kind, name in tier3:
    count += 1
    if not re.fullmatch(r"workflow_execution_service_[a-z0-9_]+", name):
        errors.append(f'"{name}": Tier 3 collectors must be workflow_execution_service_<snake_case>')
    if kind.startswith("Counter") and not name.endswith("_total"):
        errors.append(f'"{name}": {kind} must end in _total')
    if kind.startswith("Histogram") and not name.endswith("_seconds"):
        errors.append(f'"{name}": {kind} must end in _seconds')
    if kind.startswith("Gauge") and name.endswith("_total"):
        errors.append(f'"{name}": a gauge must not end in _total')

    # Label vocabulary compliance: extract the extra string args (label names)
    # from this constructor call. The call shape is:
    #   gincommon.New<Kind>(nil, "<name>", "<help>", "<label1>", "<label2>", ...)
    # where label names start after the help string.
    label_pat = (
        r'gincommon\.New' + re.escape(kind) + r'\(nil,\s*"'
        + re.escape(name) + r'",\s*"[^"]*"((?:,\s*"[^"]*")*)\)'
    )
    for labels_str in re.findall(label_pat, tier3_body, re.DOTALL):
        check_label_names(name, re.findall(r'"([^"]+)"', labels_str))

if errors:
    for e in errors:
        print(f"::error::{e}")
    print(f"\n{len(errors)} violation(s) across {count} collectors.")
    sys.exit(1)

print(
    f"  ✔  {count} collectors — all built with gincommon constructors;"
    f" naming, suffix and label vocabulary rules OK"
)
PYEOF
