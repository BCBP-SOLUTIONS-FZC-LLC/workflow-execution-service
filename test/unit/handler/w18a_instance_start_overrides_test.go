package handler_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func w18aTwoStageWorld(t *testing.T) *w18aWorld {
	t.Helper()
	return w18aNewWorld(t, w18aPlanJSON(t, w18aStage("review", uuid.New()), w18aStage("sign", uuid.New())))
}

func TestStartInstance_OmittingContextAndOverrideMap_Returns201AndStoresAnEmptyOverrideMap(t *testing.T) {
	world := w18aTwoStageWorld(t)

	rec := do(world.router, world.startRequest())

	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var body w18aSummary
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, "RUNNING", body.Status)
	assert.Equal(t, testTenantID, body.TenantID)
	assert.Equal(t, w18aSubject, body.SubjectID)
	require.Len(t, world.instances.creates, 1)
	stored := world.instances.creates[0]
	assert.Equal(t, body.ID, stored.ID)
	assert.Equal(t, testTenantID, stored.TenantID)
	assert.JSONEq(t, `{}`, string(stored.OverrideMap), "the override map is stored as an empty object")
	assert.Empty(t, stored.ContextJSON, "no context was sent, none is stored")
	require.Len(t, world.temporal.starts, 1)
	assert.Equal(t, body.ID, world.temporal.starts[0].InstanceID)
	assert.Empty(t, world.temporal.starts[0].OverrideMap)
	assert.Empty(t, world.temporal.starts[0].ContextJSON)
}

type w18aSummary struct {
	ID        uuid.UUID `json:"id"`
	TenantID  uuid.UUID `json:"tenant_id"`
	SubjectID string    `json:"subject_id"`
	Status    string    `json:"status"`
}

func TestStartInstance_OverrideMapDiffersFromTheLiveInstanceWithTheSameKey_Returns409DuplicateBusinessKey(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	started := map[string]uuid.UUID{"finance/review": first, "finance/sign": second}
	cases := []struct {
		name    string
		changed map[string]uuid.UUID
	}{
		{"one entry fewer", map[string]uuid.UUID{"finance/review": first}},
		{"another user for a node", map[string]uuid.UUID{"finance/review": first, "finance/sign": uuid.New()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w18aTwoStageWorld(t)
			request := func(overrides map[string]uuid.UUID) *http.Request {
				return req(http.MethodPost, instancesPath(), map[string]any{
					"subject_id": w18aSubject, "business_key": w18aBizKey, "workflow_version_id": world.versionID,
					"override_map": overrides,
				})
			}
			rec := do(world.router, request(started))
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			startedRows := world.instances.committedCount()

			rec = do(world.router, request(tc.changed))

			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			var body struct {
				Type string `json:"type"`
				Code string `json:"code"`
			}
			decodeJSON(t, rec.Body, &body)
			assert.Equal(t, "DUPLICATE_BUSINESS_KEY", body.Code)
			assert.Regexp(t, `/duplicate-business-key$`, body.Type)
			assert.Equal(t, startedRows, world.instances.committedCount(), "no second instance is stored")
			assert.Len(t, world.outbox.enqueued, 1, "no second started event")
			assert.Len(t, world.temporal.starts, 1, "no second workflow")
			for _, inst := range world.instances.committed {
				assert.JSONEq(t, `{"finance/review":"`+first.String()+`","finance/sign":"`+second.String()+`"}`, string(inst.OverrideMap), "the live instance keeps its override map")
			}
		})
	}

	t.Run("control: the same override map is the same start", func(t *testing.T) {
		world := w18aTwoStageWorld(t)
		request := func() *http.Request {
			return req(http.MethodPost, instancesPath(), map[string]any{
				"subject_id": w18aSubject, "business_key": w18aBizKey, "workflow_version_id": world.versionID,
				"override_map": started,
			})
		}
		require.Equal(t, http.StatusCreated, do(world.router, request()).Code)

		rec := do(world.router, request())

		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		assert.Equal(t, 1, world.instances.committedCount())
		assert.Len(t, world.temporal.starts, 1)
	})
}
