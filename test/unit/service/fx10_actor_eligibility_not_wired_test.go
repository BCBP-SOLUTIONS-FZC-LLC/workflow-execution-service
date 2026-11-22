package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// fx10TaskAction is one of the three actions of a task's own assignee; each one checks the actor's eligibility.
type fx10TaskAction struct {
	name string
	// build returns a harness whose task the caller may act on.
	build func(t *testing.T) *taHarness
	run   func(h *taHarness) (*port.Task, error)
}

func fx10TaskActions() []fx10TaskAction {
	plain := func(mode string) func(t *testing.T) *taHarness {
		return func(t *testing.T) *taHarness { return newTaHarness(t, mode) }
	}
	return []fx10TaskAction{
		{"claim", plain("all"), func(h *taHarness) (*port.Task, error) {
			return h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)
		}},
		{"complete", plain("single"), func(h *taHarness) (*port.Task, error) {
			return h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, json.RawMessage(`{}`), csdTaskVersion)
		}},
		{"defer", func(t *testing.T) *taHarness {
			h, _ := newTaDeferHarness(t, 0)
			return h
		}, func(h *taHarness) (*port.Task, error) {
			return h.svc.Defer(context.Background(), h.tenantID, h.task.ID, h.callerID, "waiting for documents", csdTaskVersion)
		}},
	}
}

func TestTaskService_ActorActions_BuiltWithoutWhatTheEligibilityCheckNeeds_RefuseAndChangeNothing(t *testing.T) {
	for _, a := range fx10TaskActions() {
		for _, w := range fx9IncompleteWirings() {
			t.Run(a.name+"/"+w.name, func(t *testing.T) {
				h := a.build(t)
				if !w.withEligibility {
					h.svc.Eligibility = nil
				}
				if !w.withDefinitions {
					h.svc.Definitions = nil
				}

				got, err := a.run(h)

				require.Error(t, err)
				assert.ErrorContains(t, err, "eligibility checker", "the refusal names what is missing")
				assert.Nil(t, got)
				assert.Empty(t, h.temporalLog.updates, "no update may be sent")
				assert.Empty(t, h.elig.calls)
				h.requireNothingChanged(t)
				h.requireTaskUntouched(t)
			})
		}
	}
}

func TestTaskService_ActorActions_BuiltWithTheEligibilityCheck_AskItAboutTheCaller(t *testing.T) {
	for _, a := range fx10TaskActions() {
		t.Run(a.name, func(t *testing.T) {
			h := a.build(t)

			_, err := a.run(h)

			require.NoError(t, err)
			require.Len(t, h.elig.calls, 1)
			assert.Equal(t, h.callerID, h.elig.calls[0].UserID)
			assert.Equal(t, h.tenantID, h.elig.calls[0].TenantID)
		})
	}
}
