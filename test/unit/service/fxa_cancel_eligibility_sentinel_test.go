package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestCancelDelegation_AnEligibilityReadFailureIsTheEligibilityReadSentinel(t *testing.T) {
	cases := []struct {
		name         string
		failPlan     bool
		wantEligible bool
	}{
		{name: "the delegator's eligibility cannot be read", wantEligible: true},
		{name: "the compiled plan cannot be read", failPlan: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _, _ := w7bClient(t)
			delegator := uuid.New()
			held := h.seed(t, "reviewer")
			held.assignment.Reason = dcrHop(uuid.New(), delegator)
			h.svc.Definitions = h.definitions
			h.svc.Eligibility = &fakeEligibilityChecker{check: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (bool, error) {
				return false, assert.AnError
			}}
			if tc.failPlan {
				h.definitions.failing[held.versionID] = assert.AnError
			}

			_, err := h.cancel()

			require.ErrorIs(t, err, assert.AnError)
			assert.Equal(t, tc.wantEligible, errors.Is(err, port.ErrEligibilityRead))
		})
	}
}
