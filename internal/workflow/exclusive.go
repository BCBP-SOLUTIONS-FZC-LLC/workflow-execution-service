package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func evaluateCondition(expr, resultJSON string) (bool, error) {
	condition, err := dsl.ParseCondition(expr)
	if err != nil {
		return false, fmt.Errorf("workflow: %w", err)
	}

	var result map[string]any
	if resultJSON != "" {
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			return false, fmt.Errorf("workflow: parsing result_json for condition %q: %w", expr, err)
		}
	}
	return condition.Matches(result), nil
}

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
