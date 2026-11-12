package contract_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/contract"
)

// The department scope of pause-tasks is optional and additive: both bodies
// below must satisfy the internal spec, and a malformed id gets the route's
// existing 400.
func TestInternalContract_PauseTasksDepartment(t *testing.T) {
	spec := contract.Load(t, internalSpec)
	tenantID, userID, deptID := uuid.New(), uuid.New(), uuid.New()
	path := "/api/v1/internal/users/" + userID.String() + "/pause-tasks"

	t.Run("with a department", func(t *testing.T) {
		p := &pauser{}
		run(t, spec, internalRouter(&guard{}, p), exchange{
			method: http.MethodPost, path: path, identity: true, want: http.StatusNoContent, validReq: true,
			body: `{"tenant_id":"` + tenantID.String() + `","department_id":"` + deptID.String() + `"}`,
		})
		if p.department == nil || *p.department != deptID {
			t.Errorf("pauser got department %v, want %s", p.department, deptID)
		}
	})

	t.Run("without a department", func(t *testing.T) {
		p := &pauser{}
		run(t, spec, internalRouter(&guard{}, p), exchange{
			method: http.MethodPost, path: path, identity: true, want: http.StatusNoContent, validReq: true,
			body: `{"tenant_id":"` + tenantID.String() + `"}`,
		})
		if p.department != nil {
			t.Errorf("pauser got department %s, want none", p.department)
		}
	})

	t.Run("department is not a uuid", func(t *testing.T) {
		p := &pauser{}
		run(t, spec, internalRouter(&guard{}, p), exchange{
			method: http.MethodPost, path: path, identity: true, want: http.StatusBadRequest,
			body: `{"tenant_id":"` + tenantID.String() + `","department_id":"nope"}`,
		})
		if p.got != [2]uuid.UUID{} {
			t.Errorf("a refused request reached the pauser: %v", p.got)
		}
	})
}
