package workflow

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// evaluateCondition evaluates a single binary equality/inequality expression
// of the form `<field> == "<literal>"` or `<field> != "<literal>"` against
// the just-completed stage's result_json.
//
// This is the interpreter's permanent scope limit for exclusive-gateway
// conditions (LLD §2.6, Appendix A.1 #6): no expression engine, no
// third-party dependency. Richer boolean/arithmetic expressions are a
// deliberately rejected design, not an oversight — do not extend this.
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

// selectBranch picks the winning ExclusiveBranch per LLD §2.6: the first
// branch (in array order) whose non-empty ConditionExpression evaluates
// true, or — if none match — the single branch with an empty
// ConditionExpression (the implicit else, backed by BPMN's own default-flow
// marker; the compiler enforces exactly one conditionless flow per
// gateway). Returns nil if no branch matches and no implicit else exists.
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
