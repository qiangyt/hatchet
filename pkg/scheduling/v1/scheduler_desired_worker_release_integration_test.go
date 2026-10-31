//go:build integration

package v1_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/config/database"
	repo "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	schedv1 "github.com/hatchet-dev/hatchet/pkg/scheduling/v1"
)

// workflowWithStep identifies the single step of a registered workflow version.
type workflowWithStep struct {
	workflowId        uuid.UUID
	workflowVersionId uuid.UUID
	stepId            uuid.UUID
}

// createWorkflowWithConcurrency registers a workflow version whose single step
// runs the given action, optionally carrying step-level concurrency entries,
// and returns its identifiers.
func createWorkflowWithConcurrency(
	t *testing.T,
	ctx context.Context,
	conf *database.Layer,
	tenantId uuid.UUID,
	name string,
	entries []repo.CreateConcurrencyOpts,
) *workflowWithStep {
	t.Helper()

	desc := "test workflow"
	wfVersion, err := conf.V1.Workflows().PutWorkflowVersion(ctx, tenantId, &repo.CreateWorkflowVersionOpts{
		Name:        name,
		Description: &desc,
		Tasks: []repo.CreateStepOpts{
			{
				ReadableId:  "my-task",
				Action:      "test:run",
				Concurrency: entries,
			},
		},
	})
	require.NoError(t, err)

	steps, err := conf.V1.Workflows().ListStepsByWorkflowVersionId(ctx, tenantId, wfVersion.WorkflowVersion.ID)
	require.NoError(t, err)
	require.Len(t, steps, 1)

	return &workflowWithStep{
		workflowId:        wfVersion.WorkflowVersion.WorkflowId,
		workflowVersionId: wfVersion.WorkflowVersion.ID,
		stepId:            steps[0].ID,
	}
}

// createTenantWorker adds another worker (with its own dispatcher) to an
// existing tenant, mirroring createTenantDispatcherWorker's setup.
func createTenantWorker(
	t *testing.T,
	ctx context.Context,
	r repo.Repository,
	tenantId uuid.UUID,
	name string,
	maxRuns int,
	actions []string,
) uuid.UUID {
	t.Helper()

	dispatcherId := uuid.New()
	_, err := r.Dispatcher().CreateNewDispatcher(ctx, &repo.CreateDispatcherOpts{ID: dispatcherId})
	require.NoError(t, err)

	worker, err := r.Workers().CreateNewWorker(ctx, tenantId, &repo.CreateWorkerOpts{
		DispatcherId: dispatcherId,
		Name:         name,
		Services:     []string{},
		Actions:      actions,
		SlotConfig:   map[string]int32{repo.SlotTypeDefault: int32(maxRuns)},
	})
	require.NoError(t, err)

	require.NoError(t, r.Workers().UpdateWorkerHeartbeat(ctx, tenantId, worker.ID, time.Now().UTC()))

	isActive := true
	isPaused := false
	_, err = r.Workers().UpdateWorker(ctx, tenantId, worker.ID, &repo.UpdateWorkerOpts{
		IsActive: &isActive,
		IsPaused: &isPaused,
	})
	require.NoError(t, err)

	return worker.ID
}

// createStickyDesiredWorkerTask inserts a QUEUED task carrying sticky=HARD and a
// desired worker, so the v1_task_insert_trigger produces the queue item the
// scheduler ranks against.
func createStickyDesiredWorkerTask(
	t *testing.T,
	ctx context.Context,
	conf *database.Layer,
	tenantId uuid.UUID,
	wf *workflowWithStep,
	desiredWorkerId uuid.UUID,
) *sqlcv1.V1Task {
	t.Helper()

	queries := sqlcv1.New()

	taskParams := newCreateTasksParams(1)
	taskParams.Tenantids[0] = tenantId
	taskParams.Queues[0] = "default"
	taskParams.Actionids[0] = "test:run"
	taskParams.Stepids[0] = wf.stepId
	taskParams.Stepreadableids[0] = "my-task"
	taskParams.Workflowids[0] = wf.workflowId
	taskParams.Scheduletimeouts[0] = "5m"
	taskParams.Priorities[0] = 1
	taskParams.Stickies[0] = string(sqlcv1.V1StickyStrategyHARD)
	taskParams.Desiredworkerids[0] = &desiredWorkerId
	taskParams.Externalids[0] = uuid.New()
	taskParams.Displaynames[0] = "sticky-task"
	taskParams.Additionalmetadatas[0] = []byte(`{}`)
	taskParams.InitialStates[0] = string(sqlcv1.V1TaskInitialStateQUEUED)
	taskParams.Concurrencyparentstrategyids[0] = []pgtype.Int8{}
	taskParams.ConcurrencyStrategyIds[0] = []int64{}
	taskParams.ConcurrencyKeys[0] = []string{}
	taskParams.WorkflowVersionIds[0] = wf.workflowVersionId
	taskParams.WorkflowRunIds[0] = uuid.New()
	taskParams.IsDagOrchestrators[0] = false

	tasks, err := queries.CreateTasks(ctx, conf.Pool, taskParams)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	return tasks[0]
}

// taskRuntimeWorkerId returns the worker the task is currently assigned to, or
// nil when it has no runtime row (i.e. it is still queued).
func taskRuntimeWorkerId(t *testing.T, ctx context.Context, conf *database.Layer, task *sqlcv1.V1Task) *uuid.UUID {
	t.Helper()

	var workerId *uuid.UUID
	err := conf.Pool.QueryRow(ctx, `
		SELECT worker_id FROM v1_task_runtime
		WHERE task_id = $1 AND task_inserted_at = $2 AND retry_count = $3
	`, task.ID, task.InsertedAt.Time, task.RetryCount).Scan(&workerId)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		require.NoError(t, err)
	}

	return workerId
}

// taskRuntimeWorkerIdOn reports whether the task currently has a runtime row on
// the given worker.
func taskRuntimeWorkerIdOn(t *testing.T, ctx context.Context, conf *database.Layer, task *sqlcv1.V1Task, workerId uuid.UUID) *uuid.UUID {
	t.Helper()

	actual := taskRuntimeWorkerId(t, ctx, conf, task)
	if actual == nil || *actual != workerId {
		return nil
	}

	return actual
}

// waitForRuntimeWorkerId polls until the task has a runtime row on the expected
// worker, failing the test if that never happens.
func waitForRuntimeWorkerId(t *testing.T, ctx context.Context, conf *database.Layer, task *sqlcv1.V1Task, expected uuid.UUID, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if workerId := taskRuntimeWorkerId(t, ctx, conf, task); workerId != nil && *workerId == expected {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	workerId := taskRuntimeWorkerId(t, ctx, conf, task)
	actual := "none (still queued)"
	if workerId != nil {
		actual = workerId.String()
	}
	require.Failf(t, "timeout", "task %d was not assigned to worker %s within %s (current runtime worker: %s)",
		task.ID, expected, timeout, actual)
}

// waitForNoRuntimeRow polls until the task has no runtime row, then keeps
// watching for the given quiet period to prove it stays unassigned.
func waitForNoRuntimeRow(t *testing.T, ctx context.Context, conf *database.Layer, task *sqlcv1.V1Task, quiet time.Duration) {
	t.Helper()

	deadline := time.Now().Add(quiet)
	for time.Now().Before(deadline) {
		require.Nil(t, taskRuntimeWorkerId(t, ctx, conf, task),
			"the task must stay unassigned while only the silent worker may run it")
		time.Sleep(25 * time.Millisecond)
	}
}

// TestScheduler_ReleasesDesiredWorkerAffinityToAnotherWorker proves the
// end-to-end fix for the stuck-affinity hole: a sticky=HARD task whose desired
// worker went silent is not assignable by anyone (the HARD branch excludes
// every other worker, and the desired worker is no longer an active one), so it
// sits in the queue. One reassignment pass clears the dead affinity, and the
// scheduler then assigns the task to the other worker that can run the action.
func TestScheduler_ReleasesDesiredWorkerAffinityToAnotherWorker(t *testing.T) {
	runWithDatabase(t, func(conf *database.Layer) error {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		requireSchedulerSchema(t, ctx, conf)

		actionA := "test:run"
		maxRuns := 1

		// Two workers able to run the same action. The desired worker's heartbeat
		// is backdated before the pool starts, so from the scheduler's point of
		// view it went silent (still isActive, i.e. a crash rather than a
		// graceful disconnect).
		tenantId, desiredWorkerId, tenant := createTenantDispatcherWorker(t, ctx, conf.V1, "scheduler-affinity-desired", maxRuns, []string{actionA})
		otherWorkerId := createTenantWorker(t, ctx, conf.V1, tenantId, "scheduler-affinity-other", maxRuns, []string{actionA})

		_, err := conf.Pool.Exec(ctx, `
			UPDATE "Worker" SET "lastHeartbeatAt" = NOW() - INTERVAL '1 hour' WHERE "id" = $1
		`, desiredWorkerId)
		require.NoError(t, err)

		wf := createWorkflowWithConcurrency(t, ctx, conf, tenantId, "scheduler-affinity-wf", nil)
		task := createStickyDesiredWorkerTask(t, ctx, conf, tenantId, wf, desiredWorkerId)

		// the lease manager only picks up queues that exist in v1_queue
		_, err = conf.Pool.Exec(ctx, `
			INSERT INTO v1_queue (tenant_id, name, last_active) VALUES ($1, 'default', NOW())
			ON CONFLICT (tenant_id, name) DO UPDATE SET last_active = NOW()
		`, tenantId)
		require.NoError(t, err)

		l := zerolog.Nop()
		pool, cleanup, err := schedv1.NewSchedulingPool(
			conf.V1.Scheduler(),
			conf.V1.Tasks(),
			newTestOutbox(t, conf),
			&l,
			100,
			20,
			10*time.Millisecond,
			50*time.Millisecond,
			50*time.Millisecond,
			100*time.Millisecond,
			5*time.Millisecond,
			false,
			1,
			true,
			nil,
		)
		require.NoError(t, err)
		defer func() { _ = cleanup() }()

		ext := &captureSnapshotsExt{
			ch:        make(chan snapshotEvent, 100),
			cleanupCh: make(chan uuid.UUID, 10),
		}
		pool.Extensions.Add(ext)

		pool.SetTenants([]*sqlcv1.Tenant{tenant})

		// the pool is live and knows the other worker's capacity
		waitForWorkerUtilization(t, ext.ch, tenantId, otherWorkerId, 5*time.Second)

		// the hole: with the desired worker silent, nobody may run the task
		waitForNoRuntimeRow(t, ctx, conf, task, 1500*time.Millisecond)

		// one reassignment pass releases the dead affinity
		res, _, err := conf.V1.Tasks().ProcessTaskReassignments(ctx, tenantId)
		require.NoError(t, err)
		require.Len(t, res.ClearedDesiredWorkerTasks, 1,
			"the pass must report the queued task whose desired worker went silent")
		assert.Equal(t, task.ID, res.ClearedDesiredWorkerTasks[0].ID)

		// the scheduler now hands the task to the other worker
		waitForRuntimeWorkerId(t, ctx, conf, task, otherWorkerId, 15*time.Second)

		// the sticky strategy survives the release (the queue item is unqueued on
		// assignment, so the task row is the surviving copy)
		var sticky string
		err = conf.Pool.QueryRow(ctx, `
			SELECT sticky::text FROM v1_task WHERE id = $1 AND inserted_at = $2
		`, task.ID, task.InsertedAt.Time).Scan(&sticky)
		require.NoError(t, err)
		assert.Equal(t, string(sqlcv1.V1StickyStrategyHARD), sticky,
			"releasing the affinity must not change the sticky strategy")

		return nil
	})
}

// TestScheduler_KeepsDesiredWorkerAffinityWhileWorkerIsLive is the control for
// the test above: the same task, with a desired worker that is still
// heartbeating, is assigned to that worker and left alone by the reassignment
// pass.
func TestScheduler_KeepsDesiredWorkerAffinityWhileWorkerIsLive(t *testing.T) {
	runWithDatabase(t, func(conf *database.Layer) error {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		requireSchedulerSchema(t, ctx, conf)

		actionA := "test:run"
		maxRuns := 1

		tenantId, desiredWorkerId, tenant := createTenantDispatcherWorker(t, ctx, conf.V1, "scheduler-affinity-live", maxRuns, []string{actionA})
		otherWorkerId := createTenantWorker(t, ctx, conf.V1, tenantId, "scheduler-affinity-live-other", maxRuns, []string{actionA})

		wf := createWorkflowWithConcurrency(t, ctx, conf, tenantId, "scheduler-affinity-live-wf", nil)
		task := createStickyDesiredWorkerTask(t, ctx, conf, tenantId, wf, desiredWorkerId)

		_, err := conf.Pool.Exec(ctx, `
			INSERT INTO v1_queue (tenant_id, name, last_active) VALUES ($1, 'default', NOW())
			ON CONFLICT (tenant_id, name) DO UPDATE SET last_active = NOW()
		`, tenantId)
		require.NoError(t, err)

		l := zerolog.Nop()
		pool, cleanup, err := schedv1.NewSchedulingPool(
			conf.V1.Scheduler(),
			conf.V1.Tasks(),
			newTestOutbox(t, conf),
			&l,
			100,
			20,
			10*time.Millisecond,
			50*time.Millisecond,
			50*time.Millisecond,
			100*time.Millisecond,
			5*time.Millisecond,
			false,
			1,
			true,
			nil,
		)
		require.NoError(t, err)
		defer func() { _ = cleanup() }()

		ext := &captureSnapshotsExt{
			ch:        make(chan snapshotEvent, 100),
			cleanupCh: make(chan uuid.UUID, 10),
		}
		pool.Extensions.Add(ext)

		pool.SetTenants([]*sqlcv1.Tenant{tenant})

		// sticky=HARD routes the task to its desired worker even though the other
		// worker has the same capacity
		waitForRuntimeWorkerId(t, ctx, conf, task, desiredWorkerId, 15*time.Second)
		require.Nil(t, taskRuntimeWorkerIdOn(t, ctx, conf, task, otherWorkerId),
			"the other worker must not receive a sticky=HARD task desired elsewhere")

		// the reassignment pass leaves a live worker's affinity alone
		res, _, err := conf.V1.Tasks().ProcessTaskReassignments(ctx, tenantId)
		require.NoError(t, err)
		assert.Empty(t, res.ClearedDesiredWorkerTasks,
			"a live desired worker must keep its affinity")

		var taskDesiredWorkerID *uuid.UUID
		err = conf.Pool.QueryRow(ctx, `
			SELECT desired_worker_id FROM v1_task WHERE id = $1 AND inserted_at = $2
		`, task.ID, task.InsertedAt.Time).Scan(&taskDesiredWorkerID)
		require.NoError(t, err)
		require.NotNil(t, taskDesiredWorkerID, "the task must keep its desired worker")
		assert.Equal(t, desiredWorkerId, *taskDesiredWorkerID,
			"a live desired worker's affinity must survive the reassignment pass")

		return nil
	})
}
