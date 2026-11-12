package domain

// SystemCaller is the caller identity every internal call between platform
// services names: senders put it in x-user-id and x-tenant-roles, and the
// internal routes require it in x-tenant-roles. The headers are not a
// credential; the mesh authenticates the calling workload.
const SystemCaller = "iam-system"
