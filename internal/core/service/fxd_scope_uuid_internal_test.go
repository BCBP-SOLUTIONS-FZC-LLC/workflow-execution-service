package service

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestScopeMatches_TenderScopeComparesUUIDsByValue(t *testing.T) {
	id := uuid.NewString()
	cases := []struct {
		name      string
		subjectID string
		scopeID   string
		want      bool
	}{
		{"same uuid, subject upper and scope lower", strings.ToUpper(id), id, true},
		{"same uuid, subject lower and scope upper", id, strings.ToUpper(id), true},
		{"same uuid, same text", id, id, true},
		{"two different uuids", id, uuid.NewString(), false},
		{"braced form of the same uuid", id, "{" + id + "}", true},
		{"urn form of the same uuid", id, "urn:uuid:" + id, true},
		{"undashed form of the same uuid", id, strings.ReplaceAll(id, "-", ""), true},
		{"nil uuid on both sides matches nothing", uuid.Nil.String(), uuid.Nil.String(), false},
		{"nil uuid subject and a real scope id", uuid.Nil.String(), id, false},
		{"real subject and a nil scope id", id, uuid.Nil.String(), false},
		{"uuid subject and non-uuid scope", id, "TND-001", false},
		{"non-uuid subject and uuid scope", "TND-001", id, false},
		{"equal non-uuid text matches nothing", "TND-001", "TND-001", false},
		{"non-uuid text in another case matches nothing", "TND-001", "tnd-001", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newW14aScopeWorld(t)
			scopeID := tc.scopeID

			assert.Equal(t, tc.want, w.matches("finance/a", w.financeID, tc.subjectID, scopeTender, &scopeID))
		})
	}
}

func TestScopeMatches_DepartmentScopeComparesUUIDsByValue(t *testing.T) {
	w := newW14aScopeWorld(t)
	finance := w.financeID.String()
	cases := []struct {
		name    string
		nodeKey string
		scopeID string
		want    bool
	}{
		{"scope id in upper case", "finance/a", strings.ToUpper(finance), true},
		{"scope id in lower case", "finance/a", finance, true},
		{"upper case with a key that holds more slashes", "finance/a/b", strings.ToUpper(finance), true},
		{"upper case with a key that has no slash", "finance", strings.ToUpper(finance), true},
		{"another department's id in upper case", "finance/a", strings.ToUpper(w.legalID.String()), false},
		{"scope id that is not a uuid", "finance/a", "finance", false},
		{"braced scope id", "finance/a", "{" + finance + "}", true},
		{"urn scope id", "finance/a", "urn:uuid:" + finance, true},
		{"nil scope id", "finance/a", uuid.Nil.String(), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scopeID := tc.scopeID

			assert.Equal(t, tc.want, w.matches(tc.nodeKey, w.financeID, "subject", "department", &scopeID))
		})
	}
}

func TestScopeMatches_DepartmentScopeWithANonUUIDPlanDepartmentIDMatchesNothing(t *testing.T) {
	w := newW14aScopeWorld(t)
	w.source.planJSON = strings.ReplaceAll(w.source.planJSON, w.financeID.String(), "FIN-1")
	scopeID := "FIN-1"

	assert.False(t, w.matches("finance/a", uuid.Nil, "subject", "department", &scopeID))
	scopeID = "fin-1"
	assert.False(t, w.matches("finance/a", uuid.Nil, "subject", "department", &scopeID))
}

func TestScopeMatches_DepartmentScopeWithTheNilUUIDOnBothSidesMatchesNothing(t *testing.T) {
	w := newW14aScopeWorld(t)
	w.source.planJSON = strings.ReplaceAll(w.source.planJSON, w.financeID.String(), uuid.Nil.String())
	scopeID := uuid.Nil.String()

	assert.False(t, w.matches("finance/a", uuid.Nil, "subject", "department", &scopeID))
}
