// Posts/updates a single sticky PR comment summarizing this CI run's job results.
// ci.yml's inline `script:` step requires this file and calls the exported
// function directly, passing `github`/`context` through. A required file's
// top-level `await` would fail outside an async wrapper — module.exports is an
// async function precisely so the caller's `await run(...)` supplies that
// wrapper; JOB_RESULTS/COVERAGE_PCT are read from process.env (set by the
// calling step, available to required files the same as the inline script).

const MARKER = "<!-- ci-pr-summary -->";

const JOB_LABELS = [
  ["generate", "Generate (sqlc/mocks)"],
  ["validate-quality", "Code quality"],
  ["validate-test", "Tests & race detector (unit + integration)"],
  ["lint-dockerfile", "Dockerfile lint"],
  ["build-image-cache", "Image build (cache)"],
  ["trivy-cve-scan", "Trivy CVE scan"],
  ["smoke-tests", "Smoke tests"],
];

function icon(result) {
  if (result === "success") return "✅";
  if (result === "skipped") return "⏭️";
  if (result === "cancelled") return "🚫";
  return "❌";
}

module.exports = async function prSummary({ github, context }) {
  const jobResults = JSON.parse(process.env.JOB_RESULTS || "{}");
  const coveragePct = process.env.COVERAGE_PCT || "n/a";
  const coverageMin = process.env.COVERAGE_MIN;
  const coverageRow = coverageMin
    ? `| ${parseFloat(coveragePct) >= parseFloat(coverageMin) ? "✅" : "❌"} | Coverage ≥ ${coverageMin}% | \`${coveragePct}\` |`
    : `| | Total coverage (the tests job holds each package to its floor) | \`${coveragePct}\` |`;

  const rows = JOB_LABELS.filter(([id]) => jobResults[id])
    .map(([id, label]) => {
      const result = jobResults[id].result;
      return `| ${icon(result)} | ${label} | \`${result}\` |`;
    })
    .join("\n");

  const results = JOB_LABELS
    .filter(([id]) => jobResults[id])
    .map(([id]) => jobResults[id].result);
  const allPassed = results.every((r) => r === "success");
  // A PR with the skip-ci label (a lower PR of a stack) or a documentation-only
  // change reports its checks as skipped; that is not a failure.
  const nothingFailed = results.every((r) => r === "success" || r === "skipped");
  const headline = allPassed
    ? "✅ All checks passed — ready to merge"
    : nothingFailed
      ? "⏭️ Checks skipped — CI runs on the top PR of the stack, or only documentation changed"
      : "❌ Some checks failed";

  const body = [
    MARKER,
    `## ${headline}`,
    "",
    "| | Check | Result |",
    "|---|---|---|",
    rows,
    coverageRow,
    "",
    "📦 Image built for `linux/amd64` · merges to `main` are signed with Cosign",
    `🔍 [Security tab](https://github.com/${context.repo.owner}/${context.repo.repo}/security)`,
  ].join("\n");

  const { data: comments } = await github.rest.issues.listComments({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: context.issue.number,
  });

  const existing = comments.find((c) => c.body?.includes(MARKER));

  if (existing) {
    await github.rest.issues.updateComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      comment_id: existing.id,
      body,
    });
  } else {
    await github.rest.issues.createComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: context.issue.number,
      body,
    });
  }
};
