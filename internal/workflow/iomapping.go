package workflow

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func applyIOMapping(contextJSON string, m *dsl.IOMapping) (string, error) {
	if m == nil || len(m.Inputs) == 0 {
		return contextJSON, nil
	}
	vars := map[string]any{}
	if contextJSON != "" {
		dec := json.NewDecoder(strings.NewReader(contextJSON))
		dec.UseNumber()
		if err := dec.Decode(&vars); err != nil {
			return "", fmt.Errorf("workflow: parsing context_json for io_mapping: %w", err)
		}
	}
	if vars == nil {
		vars = map[string]any{}
	}
	for _, in := range m.Inputs {
		vars[in.Target] = in.InputValue(vars)
	}
	out, err := json.Marshal(vars)
	if err != nil {
		return "", fmt.Errorf("workflow: marshaling context_json after io_mapping: %w", err)
	}
	return string(out), nil
}
