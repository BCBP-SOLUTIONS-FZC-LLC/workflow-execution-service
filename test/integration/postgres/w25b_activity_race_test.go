//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// w25bGate holds the first two callers until both have arrived, so two
// activities that read the task before either writes really overlap.
type w25bGate struct {
	mu      sync.Mutex
	arrived int
	release chan struct{}
}

func w25bNewGate() *w25bGate { return &w25bGate{release: make(chan struct{})} }

func (g *w25bGate) arrive(ctx context.Context) error {
	g.mu.Lock()
	g.arrived++
	if g.arrived == 2 {
		close(g.release)
	}
	g.mu.Unlock()
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// w25bGatedTasks lets the task read through only once both racing activities have made it.
type w25bGatedTasks struct {
	port.TaskRepository
	gate *w25bGate
}

func (r w25bGatedTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	task, err := r.TaskRepository.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	if err := r.gate.arrive(ctx); err != nil {
		return nil, err
	}
	return task, nil
}

type w25bRace struct {
	deps        *outboundtemporal.Deps
	assignments port.TaskAssignmentRepository
	tasks       port.TaskRepository
	outbox      port.OutboxRepository
	tenant      uuid.UUID
	instance    *domain.Instance
	task        *domain.Task
	users       [2]uuid.UUID
	held        [2]*domain.TaskAssignment
}

// w25bNewRace seeds one task held by two assignees, neither of them lead, on a
// database where row-level security is enforced.
func w25bNewRace(t *testing.T) *w25bRace {
	t.Helper()
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	r := &w25bRace{
		assignments: postgres.NewTaskAssignmentRepo(appPool),
		tasks:       postgres.NewTaskRepo(appPool),
		outbox:      postgres.NewOutboxRepo(appPool),
		tenant:      uuid.New(),
		users:       [2]uuid.UUID{uuid.New(), uuid.New()},
	}
	instances := postgres.NewInstanceRepo(appPool)
	r.deps = &outboundtemporal.Deps{
		Instances: instances, Tasks: w25bGatedTasks{TaskRepository: r.tasks, gate: w25bNewGate()}, Assignments: r.assignments,
		Outbox: r.outbox, Transactor: postgres.NewTransactor(appPool), Validator: validator,
	}
	ctx := context.Background()
	r.instance = newInstance(r.tenant, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, r.instance))
	r.task = newTask(r.tenant, r.instance.ID)
	r.task.AssigneeMode = "all"
	r.task.Status = domain.TaskStatusInProgress
	require.NoError(t, r.tasks.Create(withGUC(ctx, r.tenant), r.task))
	for i, user := range r.users {
		r.held[i] = newTaskAssignment(r.tenant, r.task.ID, user)
		require.NoError(t, r.assignments.Create(withGUC(ctx, r.tenant), r.held[i]))
	}
	return r
}

// runBoth starts both calls together, each as its own activity attempt.
func (r *w25bRace) runBoth(first, second func(ctx context.Context) error) [2]error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var results [2]error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); results[0] = first(ctx) }()
	go func() { defer wg.Done(); results[1] = second(ctx) }()
	wg.Wait()
	return results
}

// eventUsers returns, for the events of one type, the user each one names under key.
func (r *w25bRace) eventUsers(t *testing.T, eventType, key string) []uuid.UUID {
	t.Helper()
	records, _, err := r.outbox.ListByInstance(context.Background(), r.tenant, r.instance.ID, port.PageRequest{Limit: 100})
	require.NoError(t, err)
	var users []uuid.UUID
	for _, rec := range records {
		if rec.EventType != eventType {
			continue
		}
		var stored struct {
			Data map[string]json.RawMessage `json:"data"`
		}
		require.NoError(t, json.Unmarshal(rec.Payload, &stored))
		var user uuid.UUID
		require.NoError(t, json.Unmarshal(stored.Data[key], &user))
		users = append(users, user)
	}
	return users
}

// settle splits two results into the one that succeeded and the one that must be retried.
func w25bSettle(t *testing.T, results [2]error) (winner, loser int) {
	t.Helper()
	switch {
	case results[0] == nil && results[1] != nil:
		return 0, 1
	case results[0] != nil && results[1] == nil:
		return 1, 0
	}
	require.Failf(t, "exactly one call must succeed", "results: %v", results)
	return 0, 0
}

func w25bRequireRetryable(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.False(t, errors.As(err, &appErr), "a plain error is retried by Temporal; got application error %v", err)
}

func TestClaimAssignment_TwoAssigneesClaimTogether_TheLoserLosesTheVersionRaceThenRetriesAndTakesTheLead(t *testing.T) {
	r := w25bNewRace(t)
	claim := func(i int) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			return r.deps.ClaimAssignment(ctx, port.ClaimAssignmentInput{
				TenantID: r.tenant.String(), AssignmentID: r.held[i].ID.String(), UserID: r.users[i].String(),
			})
		}
	}
	startVersion := r.task.RecordVersion

	results := r.runBoth(claim(0), claim(1))

	winner, loser := w25bSettle(t, results)
	w25bRequireRetryable(t, results[loser])
	require.ErrorIs(t, results[loser], domain.ErrRecordVersionConflict)
	ctx := context.Background()
	active, err := r.assignments.ListActiveByTask(ctx, r.tenant, r.task.ID)
	require.NoError(t, err)
	leads := w25bLeadUsers(active)
	assert.Equal(t, []uuid.UUID{r.users[winner]}, leads, "after the race only the winner leads")
	assert.Equal(t, []uuid.UUID{r.users[winner]}, r.eventUsers(t, domain.EventWorkflowTaskClaimed, "claimed_by_user_id"), "the loser's attempt left no event")

	require.NoError(t, claim(loser)(ctx), "the retry reads the new version and goes through")

	active, err = r.assignments.ListActiveByTask(ctx, r.tenant, r.task.ID)
	require.NoError(t, err)
	assert.Len(t, active, 2, "neither assignee lost their assignment")
	assert.Equal(t, []uuid.UUID{r.users[loser]}, w25bLeadUsers(active), "the retry cleared the first lead and took over")
	assert.ElementsMatch(t, []uuid.UUID{r.users[0], r.users[1]}, r.eventUsers(t, domain.EventWorkflowTaskClaimed, "claimed_by_user_id"), "one claimed event per claim")
	stored, err := r.tasks.GetByID(ctx, r.tenant, r.task.ID)
	require.NoError(t, err)
	assert.Equal(t, startVersion+2, stored.RecordVersion, "each successful claim moved the version once")

	require.NoError(t, claim(loser)(ctx), "a repeat of the committed claim changes nothing")
	assert.Len(t, r.eventUsers(t, domain.EventWorkflowTaskClaimed, "claimed_by_user_id"), 2)
	stored, err = r.tasks.GetByID(ctx, r.tenant, r.task.ID)
	require.NoError(t, err)
	assert.Equal(t, startVersion+2, stored.RecordVersion)

	otherTenant := uuid.New()
	err = r.deps.ClaimAssignment(ctx, port.ClaimAssignmentInput{TenantID: otherTenant.String(), AssignmentID: r.held[loser].ID.String()})
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr, "another tenant cannot see the assignment")
	assert.Equal(t, "NotFoundError", appErr.Type())
	assert.True(t, appErr.NonRetryable())
}

func w25bLeadUsers(active []*domain.TaskAssignment) []uuid.UUID {
	var out []uuid.UUID
	for _, a := range active {
		if a.IsLead {
			out = append(out, a.UserID)
		}
	}
	return out
}

func TestCompleteAssignment_TwoAssigneesCompleteTogether_OnlyOneCompletionIsRecordedAndTheOtherCallIsRetried(t *testing.T) {
	r := w25bNewRace(t)
	complete := func(i int) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			_, err := r.deps.CompleteAssignment(ctx, port.CompleteAssignmentInput{
				TenantID: r.tenant.String(), TaskID: r.task.ID.String(), UserID: r.users[i].String(), ResultJSON: `{"ok":true}`,
			})
			return err
		}
	}
	startVersion := r.task.RecordVersion

	results := r.runBoth(complete(0), complete(1))

	_, loser := w25bSettle(t, results)
	w25bRequireRetryable(t, results[loser])
	ctx := context.Background()

	require.NoError(t, complete(loser)(ctx), "the retry finds the task already finished")

	active, err := r.assignments.ListActiveByTask(ctx, r.tenant, r.task.ID)
	require.NoError(t, err)
	assert.Empty(t, active, "nobody is left on the task")
	var completedBy []uuid.UUID
	for _, held := range r.held {
		got, err := r.assignments.GetByID(ctx, r.tenant, held.ID)
		require.NoError(t, err)
		if got.CompletedAt != nil {
			completedBy = append(completedBy, got.UserID)
			assert.JSONEq(t, `{"ok":true}`, string(got.ResultJSON))
		}
	}
	require.Len(t, completedBy, 1, "exactly one assignment holds a recorded completion")
	assert.Equal(t, completedBy, r.eventUsers(t, domain.EventWorkflowTaskCompleted, "completed_by_user_id"), "one completed event, for the user whose completion was recorded")
	stored, err := r.tasks.GetByID(ctx, r.tenant, r.task.ID)
	require.NoError(t, err)
	assert.Equal(t, startVersion+1, stored.RecordVersion, "the task version moved once")

	require.NoError(t, complete(0)(ctx))
	require.NoError(t, complete(1)(ctx))
	assert.Len(t, r.eventUsers(t, domain.EventWorkflowTaskCompleted, "completed_by_user_id"), 1, "repeats announce nothing more")
	stored, err = r.tasks.GetByID(ctx, r.tenant, r.task.ID)
	require.NoError(t, err)
	assert.Equal(t, startVersion+1, stored.RecordVersion)
}
