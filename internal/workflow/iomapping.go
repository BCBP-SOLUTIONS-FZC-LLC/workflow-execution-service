package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// applyIOMapping applies m.Inputs to contextJSON, on entry to an inlined
// callActivity segment only (LLD §2.1). Outputs are never re-applied on
// exit — the segment's own steps write context directly. The only real
// usage today is department remapping (definition_service's
// ValidateCallActivities requires a dept_id/target="Depts" input); don't
// invent handling beyond simple source->target key copies.
func applyIOMapping(contextJSON string, m *dsl.IOMapping) (string, error) {
	if m == nil || len(m.Inputs) == 0 {
		return contextJSON, nil
	}
	vars := map[string]any{}
	if contextJSON != "" {
		if err := json.Unmarshal([]byte(contextJSON), &vars); err != nil {
			return "", fmt.Errorf("workflow: parsing context_json for io_mapping: %w", err)
		}
	}
	for _, in := range m.Inputs {
		vars[in.Target] = vars[in.Source]
	}
	out, err := json.Marshal(vars)
	if err != nil {
		return "", fmt.Errorf("workflow: marshaling context_json after io_mapping: %w", err)
	}
	return string(out), nil
}
