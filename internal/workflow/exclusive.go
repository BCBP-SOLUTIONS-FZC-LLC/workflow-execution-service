package workflow

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// evaluateCondition evaluates a single binary equality/inequality expression
// (`<field> == "<literal>"` or `!=`) against the just-completed stage's
// result_json — the interpreter's permanent scope limit for exclusive-gateway
// conditions (LLD §2.6, Appendix A.1 #6). No expression engine: do not extend this.
func evaluateCondition(expr, resultJSON string) (bool, error) {
	op := "=="
	parts := strings.SplitN(expr, "==", 2)
	if len(parts) != 2 {
		op = "!="
		parts = strings.SplitN(expr, "!=", 2)
		if len(parts) != 2 {
			return false, fmt.Errorf("workflow: condition expression %q is not a supported binary equality/inequality check", expr)
		}
	}
	field := strings.TrimSpace(parts[0])
	literal := strings.Trim(strings.TrimSpace(parts[1]), `"`)

	var result map[string]any
	if resultJSON != "" {
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			return false, fmt.Errorf("workflow: parsing result_json for condition %q: %w", expr, err)
		}
	}
	actual := fmt.Sprintf("%v", result[field])

	switch op {
	case "==":
		return actual == literal, nil
	default: // "!="
		return actual != literal, nil
	}
}

// selectBranch picks the winning ExclusiveBranch per LLD §2.6 — the compiler
// enforces exactly one conditionless flow per gateway (the implicit else).
// Returns nil if no branch matches and no implicit else exists.
func selectBranch(branches []dsl.ExclusiveBranch, resultJSON string) (*dsl.ExclusiveBranch, error) {
	var implicitElse *dsl.ExclusiveBranch
	for i := range branches {
		b := &branches[i]
		if b.ConditionExpression == "" {
			if implicitElse == nil {
				implicitElse = b
			}
			continue
		}
		ok, err := evaluateCondition(b.ConditionExpression, resultJSON)
		if err != nil {
			return nil, err
		}
		if ok {
			return b, nil
		}
	}
	return implicitElse, nil
}
