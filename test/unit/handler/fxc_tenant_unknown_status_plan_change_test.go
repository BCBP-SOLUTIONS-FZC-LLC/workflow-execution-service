package handler_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func fxcTenantPlanBody(at time.Time, status, plan, previousPlan string) map[string]any {
	return envelope("TenantStateChanged", uuid.New(), testTenantID, at, map[string]any{
		"tenant_id": testTenantID.String(), "status": status, "previous_status": "active",
		"plan": plan, "previous_plan": previousPlan, "changed_at": at.Format(time.RFC3339), "cause": "x",
	})
}

func TestHandleInternalEvent_TenantStateChanged_UnknownStatusWithAPlanChange_AppliesThePlanChangeOnly(t *testing.T) {
	for _, status := range []string{"frozen", "Suspended", ""} {
		t.Run(status, func(t *testing.T) {
			fakes := newEventsFakes()
			var applied []port.TenantLifecycleInput
			fakes.tenantLifecycle.apply = func(_ context.Context, in port.TenantLifecycleInput) error {
				applied = append(applied, in)
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))
			newer := time.Now().Truncate(time.Second)
			earlier := newer.Add(-time.Hour)

			w := postEvent(router, fxcTenantPlanBody(newer, status, "enterprise", "pro"))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, applied, 1, "the plan change reaches the reconciler")
			assert.Equal(t, testTenantID, applied[0].TenantID)
			assert.Equal(t, status, applied[0].Status)
			assert.Equal(t, "enterprise", applied[0].Plan)
			assert.Equal(t, "pro", applied[0].PreviousPlan)
			assert.Empty(t, fakes.recency.last, "the unknown status commits no recency")

			require.Equal(t, http.StatusOK, postEvent(router, tenantStateBody(uuid.New(), testTenantID, earlier, "suspended")).Code)
			require.Len(t, applied, 2, "an earlier valid status event still applies")
			assert.Equal(t, "suspended", applied[1].Status)
		})
	}
}
