package domain

// PlanEnterprise is the tenant plan tier that gets its own isolated
// Temporal task queue (LLD §3.2, §4.6); every other plan value shares
// wf-queue-default.
const PlanEnterprise = "enterprise"
