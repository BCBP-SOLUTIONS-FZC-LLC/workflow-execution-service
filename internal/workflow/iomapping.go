package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func applyIOMapping(contextJSON string, m *dsl.IOMapping, byInputRule bool) (string, error) {
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
		if byInputRule {
			vars[in.Target] = in.InputValue(vars)
		} else {
			vars[in.Target] = vars[in.Source]
		}
	}
	out, err := json.Marshal(vars)
	if err != nil {
		return "", fmt.Errorf("workflow: marshaling context_json after io_mapping: %w", err)
	}
	return string(out), nil
}
