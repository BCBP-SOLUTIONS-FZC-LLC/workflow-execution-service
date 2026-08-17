// Package valkeystream wraps the Valkey Stream consumer-group primitives
// (XADD/XREADGROUP/XACK/XAUTOCLAIM) cmd/connector-worker needs to dispatch
// connector-typed tasks (LLD workflow_connectors.md §6.5 step 0 / Decision
// #10). The existing internal/adapter/outbound/valkey.Cache only wraps the
// plain KV surface — Streams are a different enough concern (consumer
// groups, ack semantics, pending-entry reclaim) to warrant their own package
// rather than growing that one.
package valkeystream

// Entry is one Stream entry: the ID needed for Ack, and its field map.
type Entry struct {
	ID     string
	Fields map[string]string
}
