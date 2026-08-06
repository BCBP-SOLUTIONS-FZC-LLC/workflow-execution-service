package domain

// NodeKey is an opaque, comparable identifier for a single node in a compiled
// workflow plan (one department + stage/step combination). internal/workflow
// constructs concrete values; this package only fixes the type so it can be
// used as a map key and threaded through port inputs/outputs uniformly.
type NodeKey string
