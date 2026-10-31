//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlchelpers"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// This file pins the semantics of the second tier of ProcessTaskReassignments:
// a task that is still queued (no runtime row, so the first tier cannot see it)
// but whose desired worker has gone away must have its worker affinity cleared
// on both v1_queue_item (what the scheduler ranks against) and v1_task (what
// the retry trigger copies into the next queue item). The first tier's retry
// path for tasks parked on a dead worker's runtime is regression-protected
// here as well, including the requeued item it leaves behind.

// reassignTestWorkerOpts controls the connection state of a worker row inserted
// by the tests below. The defaults (zero values) are deliberately unusable:
// every test states the heartbeat age and the listener/pause flags it needs.
type reassignTestWorkerOpts struct {
	lastHeartbeatAt time.Time
	isActive        bool
	isPaused        bool
}

func createReassignTestTaskRepository(pool *pgxpool.Pool) *TaskRepositoryImpl {
	repo := newBatchTestRepository(pool)
	repo.maxInternalRetryCount = 3
	repo.reassignLimit = 1000
	repo.retryQueueLimit = 1000
	return repo
}

func insertReassignTestTenant(t *testing.T, pool *pgxpool.Pool, label string) uuid.UUID {
	t.Helper()

	tenantId := uuid.New()
	_, err := pool.Exec(ctx(t), `
		INSERT INTO "Tenant" ("id", "name", "slug", "version")
		VALUES ($1, $2, $3, 'V1')
	`, tenantId, label, label+"-"+tenantId.String())
	require.NoError(t, err)

	return tenantId
}

func insertReassignTestWorker(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID, opts reassignTestWorkerOpts) uuid.UUID {
	t.Helper()

	dispatcherId := uuid.New()
	_, err := pool.Exec(ctx(t), `INSERT INTO "Dispatcher" ("id") VALUES ($1)`, dispatcherId)
	require.NoError(t, err)

	workerId := uuid.New()
	_, err = pool.Exec(ctx(t), `
		INSERT INTO "Worker" ("id", "tenantId", "name", "dispatcherId", "lastHeartbeatAt", "isActive", "isPaused")
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, workerId, tenantId, "worker-"+workerId.String(), dispatcherId, opts.lastHeartbeatAt, opts.isActive, opts.isPaused)
	require.NoError(t, err)

	return workerId
}

// insertReassignTestQueuedTask creates a QUEUED task through insertTasks so the
// v1_task_insert_trigger produces the real v1_queue_item row, and returns the
// created task.
func insertReassignTestQueuedTask(t *testing.T, pool *pgxpool.Pool, repo *TaskRepositoryImpl, tenantId uuid.UUID, desiredWorkerId *uuid.UUID) *sqlcv1.V1Task {
	t.Helper()

	stepId := uuid.New()
	stepConfig := &sqlcv1.ListStepsByIdsRow{
		ID:                stepId,
		ReadableId:        sqlchelpers.TextFromStr("reassign-test-step"),
		TenantId:          tenantId,
		JobId:             uuid.New(),
		ActionId:          "reassign-test-action",
		Timeout:           sqlchelpers.TextFromStr("PT1M"),
		ScheduleTimeout:   "PT1M",
		WorkflowVersionId: uuid.New(),
		WorkflowId:        uuid.New(),
		DefaultPriority:   1,
		WorkflowVersionSticky: sqlcv1.NullStickyStrategy{
			StickyStrategy: sqlcv1.StickyStrategyHARD,
			Valid:          true,
		},
	}

	created, err := repo.sharedRepository.insertTasks(ctx(t), repo.pool, tenantId, []CreateTaskOpts{{
		ExternalId:      uuid.New(),
		WorkflowRunId:   uuid.New(),
		StepId:          stepId,
		Input:           &TaskInput{Input: map[string]interface{}{}},
		StepIndex:       0,
		InitialState:    sqlcv1.V1TaskInitialStateQUEUED,
		DesiredWorkerId: desiredWorkerId,
	}}, map[uuid.UUID]*sqlcv1.ListStepsByIdsRow{stepId: stepConfig})
	require.NoError(t, err)
	require.Len(t, created, 1)

	return created[0].V1Task
}

// insertReassignTestRuntime parks the task on a worker's runtime row, the state
// a task is in once it has been assigned (and the only state the first tier of
// the reassignment pass can see).
func insertReassignTestRuntime(t *testing.T, pool *pgxpool.Pool, task *sqlcv1.V1Task, tenantId, workerId uuid.UUID) {
	t.Helper()

	_, err := pool.Exec(ctx(t), `
		INSERT INTO v1_task_runtime (task_id, task_inserted_at, retry_count, worker_id, tenant_id, timeout_at)
		VALUES ($1, $2, $3, $4, $5, NOW() + INTERVAL '1 hour')
	`, task.ID, task.InsertedAt.Time, task.RetryCount, workerId, tenantId)
	require.NoError(t, err)
}

type reassignTestQueueItem struct {
	DesiredWorkerID *uuid.UUID
	Sticky          string
	RetryCount      int32
}

// getReassignTestQueueItem returns the queue item for the task's given retry
// count, or nil when no such row exists.
func getReassignTestQueueItem(t *testing.T, pool *pgxpool.Pool, task *sqlcv1.V1Task, retryCount int32) *reassignTestQueueItem {
	t.Helper()

	var item reassignTestQueueItem
	err := pool.QueryRow(ctx(t), `
		SELECT desired_worker_id, sticky::text, retry_count
		FROM v1_queue_item
		WHERE task_id = $1 AND task_inserted_at = $2 AND retry_count = $3
	`, task.ID, task.InsertedAt.Time, retryCount).Scan(&item.DesiredWorkerID, &item.Sticky, &item.RetryCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		require.NoError(t, err)
	}

	return &item
}

func reassignTestTaskColumns(t *testing.T, pool *pgxpool.Pool, task *sqlcv1.V1Task) (desiredWorkerID *uuid.UUID, retryCount int32) {
	t.Helper()

	err := pool.QueryRow(ctx(t), `
		SELECT desired_worker_id, retry_count FROM v1_task WHERE id = $1 AND inserted_at = $2
	`, task.ID, task.InsertedAt.Time).Scan(&desiredWorkerID, &retryCount)
	require.NoError(t, err)

	return desiredWorkerID, retryCount
}

// TestProcessTaskReassignmentsClearsDesiredWorkerOfDisconnectedWorker covers
// the hole the second tier exists for: a sticky=HARD task queued with a desired
// worker that stopped sending heartbeats. The first tier cannot see it (it has
// no runtime row), so only the queue-item scan can release it. Both the queue
// item and the task row must lose the dead affinity, while the sticky strategy
// and the task's queue position survive untouched.
func TestProcessTaskReassignmentsClearsDesiredWorkerOfDisconnectedWorker(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createReassignTestTaskRepository(pool)
	tenantId := insertReassignTestTenant(t, pool, "reassign-clear-stale")

	staleWorkerId := insertReassignTestWorker(t, pool, tenantId, reassignTestWorkerOpts{
		lastHeartbeatAt: time.Now().UTC().Add(-1 * time.Hour),
		isActive:        true,
	})

	task := insertReassignTestQueuedTask(t, pool, repo, tenantId, &staleWorkerId)

	res, shouldContinue, err := repo.ProcessTaskReassignments(ctx(t), tenantId)
	require.NoError(t, err)
	assert.False(t, shouldContinue, "one cleared task is far below the batch limit")

	require.Len(t, res.ClearedDesiredWorkerTasks, 1,
		"the queued task whose desired worker went silent must be reported as cleared")
	assert.Equal(t, task.ID, res.ClearedDesiredWorkerTasks[0].ID,
		"the cleared task must be identified by its id")

	desiredWorkerID, retryCount := reassignTestTaskColumns(t, pool, task)
	assert.Nil(t, desiredWorkerID, "v1_task.desired_worker_id must be cleared")
	assert.Equal(t, int32(0), retryCount, "clearing the affinity must not retry the task")

	queueItem := getReassignTestQueueItem(t, pool, task, 0)
	require.NotNil(t, queueItem, "the task must stay queued after its affinity is cleared")
	assert.Nil(t, queueItem.DesiredWorkerID, "v1_queue_item.desired_worker_id must be cleared")
	assert.Equal(t, string(sqlcv1.V1StickyStrategyHARD), queueItem.Sticky,
		"the sticky strategy must survive the affinity clear")
	assert.Equal(t, int32(0), queueItem.RetryCount, "the task must keep its original retry count")
}

// TestProcessTaskReassignmentsKeepsDesiredWorkerOfLiveWorker verifies that a
// desired worker which is still heartbeating and listening keeps its affinity:
// clearing it would break sticky routing for a healthy worker.
func TestProcessTaskReassignmentsKeepsDesiredWorkerOfLiveWorker(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createReassignTestTaskRepository(pool)
	tenantId := insertReassignTestTenant(t, pool, "reassign-keep-live")

	liveWorkerId := insertReassignTestWorker(t, pool, tenantId, reassignTestWorkerOpts{
		lastHeartbeatAt: time.Now().UTC(),
		isActive:        true,
	})

	task := insertReassignTestQueuedTask(t, pool, repo, tenantId, &liveWorkerId)

	res, shouldContinue, err := repo.ProcessTaskReassignments(ctx(t), tenantId)
	require.NoError(t, err)
	assert.False(t, shouldContinue)
	assert.Empty(t, res.ClearedDesiredWorkerTasks, "a live desired worker must keep its affinity")

	desiredWorkerID, _ := reassignTestTaskColumns(t, pool, task)
	require.NotNil(t, desiredWorkerID, "v1_task.desired_worker_id must be preserved")
	assert.Equal(t, liveWorkerId, *desiredWorkerID)

	queueItem := getReassignTestQueueItem(t, pool, task, 0)
	require.NotNil(t, queueItem)
	require.NotNil(t, queueItem.DesiredWorkerID, "v1_queue_item.desired_worker_id must be preserved")
	assert.Equal(t, liveWorkerId, *queueItem.DesiredWorkerID)
}

// TestProcessTaskReassignmentsIgnoresTasksWithoutDesiredWorker verifies that
// the scan is affinity-driven: a queued task with no desired worker is never
// touched, even when the tenant holds disconnected workers.
func TestProcessTaskReassignmentsIgnoresTasksWithoutDesiredWorker(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createReassignTestTaskRepository(pool)
	tenantId := insertReassignTestTenant(t, pool, "reassign-no-affinity")

	// A disconnected worker exists in the tenant, but the task carries no
	// affinity, so the scan has nothing to match it against.
	insertReassignTestWorker(t, pool, tenantId, reassignTestWorkerOpts{
		lastHeartbeatAt: time.Now().UTC().Add(-1 * time.Hour),
		isActive:        true,
	})

	task := insertReassignTestQueuedTask(t, pool, repo, tenantId, nil)

	res, _, err := repo.ProcessTaskReassignments(ctx(t), tenantId)
	require.NoError(t, err)
	assert.Empty(t, res.ClearedDesiredWorkerTasks,
		"a task with no desired worker must not appear in the cleared set")

	queueItem := getReassignTestQueueItem(t, pool, task, 0)
	require.NotNil(t, queueItem, "the task must stay queued")
	assert.Nil(t, queueItem.DesiredWorkerID, "the task must keep having no desired worker")
}

// TestProcessTaskReassignmentsClearsDesiredWorkerOfInactiveWorker verifies the
// graceful-disconnect path: the listener marked the worker inactive on stream
// close, so its affinity must be released even though the heartbeat is fresh.
func TestProcessTaskReassignmentsClearsDesiredWorkerOfInactiveWorker(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createReassignTestTaskRepository(pool)
	tenantId := insertReassignTestTenant(t, pool, "reassign-clear-inactive")

	inactiveWorkerId := insertReassignTestWorker(t, pool, tenantId, reassignTestWorkerOpts{
		lastHeartbeatAt: time.Now().UTC(),
		isActive:        false,
	})

	task := insertReassignTestQueuedTask(t, pool, repo, tenantId, &inactiveWorkerId)

	res, _, err := repo.ProcessTaskReassignments(ctx(t), tenantId)
	require.NoError(t, err)
	require.Len(t, res.ClearedDesiredWorkerTasks, 1,
		"a task whose desired worker deactivated its listener must be cleared")

	queueItem := getReassignTestQueueItem(t, pool, task, 0)
	require.NotNil(t, queueItem)
	assert.Nil(t, queueItem.DesiredWorkerID, "v1_queue_item.desired_worker_id must be cleared")

	desiredWorkerID, _ := reassignTestTaskColumns(t, pool, task)
	assert.Nil(t, desiredWorkerID, "v1_task.desired_worker_id must be cleared")
}

// TestProcessTaskReassignmentsClearsDesiredWorkerOfPausedWorker verifies the
// paused-worker path: a paused worker is not an assignment candidate, so its
// affinity must be released just like a disconnected one.
func TestProcessTaskReassignmentsClearsDesiredWorkerOfPausedWorker(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createReassignTestTaskRepository(pool)
	tenantId := insertReassignTestTenant(t, pool, "reassign-clear-paused")

	pausedWorkerId := insertReassignTestWorker(t, pool, tenantId, reassignTestWorkerOpts{
		lastHeartbeatAt: time.Now().UTC(),
		isActive:        true,
		isPaused:        true,
	})

	task := insertReassignTestQueuedTask(t, pool, repo, tenantId, &pausedWorkerId)

	res, _, err := repo.ProcessTaskReassignments(ctx(t), tenantId)
	require.NoError(t, err)
	require.Len(t, res.ClearedDesiredWorkerTasks, 1,
		"a task whose desired worker is paused must be cleared")

	queueItem := getReassignTestQueueItem(t, pool, task, 0)
	require.NotNil(t, queueItem)
	assert.Nil(t, queueItem.DesiredWorkerID, "v1_queue_item.desired_worker_id must be cleared")

	desiredWorkerID, _ := reassignTestTaskColumns(t, pool, task)
	assert.Nil(t, desiredWorkerID, "v1_task.desired_worker_id must be cleared")
}

// TestProcessTaskReassignmentsRetryClearsDesiredWorkerOnRequeuedItem is the
// regression protection for both tiers in one pass. A task parked on a dead
// worker's runtime goes through the first tier's retry path (retry_count
// advances), and the retry trigger requeues it with the dead worker's affinity
// copied from v1_task — the second tier must clear that affinity on the
// requeued item and on the task row, otherwise the task falls straight back
// into the hole this patch closes.
func TestProcessTaskReassignmentsRetryClearsDesiredWorkerOnRequeuedItem(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createReassignTestTaskRepository(pool)
	tenantId := insertReassignTestTenant(t, pool, "reassign-retry-requeue")

	staleWorkerId := insertReassignTestWorker(t, pool, tenantId, reassignTestWorkerOpts{
		lastHeartbeatAt: time.Now().UTC().Add(-1 * time.Hour),
		isActive:        true,
	})

	task := insertReassignTestQueuedTask(t, pool, repo, tenantId, &staleWorkerId)
	insertReassignTestRuntime(t, pool, task, tenantId, staleWorkerId)

	res, shouldContinue, err := repo.ProcessTaskReassignments(ctx(t), tenantId)
	require.NoError(t, err)
	assert.False(t, shouldContinue, "a single retried task is far below the batch limit")

	require.Len(t, res.RetriedTasks, 1, "the first tier must retry the task parked on the dead worker")
	assert.Equal(t, task.ID, res.RetriedTasks[0].Id)

	_, retryCount := reassignTestTaskColumns(t, pool, task)
	assert.Equal(t, int32(1), retryCount, "the first tier must advance the retry count")

	desiredWorkerID, _ := reassignTestTaskColumns(t, pool, task)
	assert.Nil(t, desiredWorkerID,
		"v1_task.desired_worker_id must be cleared so later retries do not re-copy the dead affinity")

	requeuedItem := getReassignTestQueueItem(t, pool, task, 1)
	require.NotNil(t, requeuedItem, "the retry trigger must have requeued the task")
	assert.Nil(t, requeuedItem.DesiredWorkerID,
		"the requeued queue item must not carry the dead worker's affinity")

	require.Len(t, res.ClearedDesiredWorkerTasks, 1,
		"the requeued item's cleared affinity must be reported for monitoring")
	assert.Equal(t, task.ID, res.ClearedDesiredWorkerTasks[0].ID)
}

// TestProcessTaskReassignmentsClearsDesiredWorkerOnlyForOwnTenant verifies the
// tenant scoping of the scan: another tenant's stale affinity must survive.
func TestProcessTaskReassignmentsClearsDesiredWorkerOnlyForOwnTenant(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createReassignTestTaskRepository(pool)

	tenantId := insertReassignTestTenant(t, pool, "reassign-own-tenant")
	neighborTenantId := insertReassignTestTenant(t, pool, "reassign-neighbor-tenant")

	ownWorkerId := insertReassignTestWorker(t, pool, tenantId, reassignTestWorkerOpts{
		lastHeartbeatAt: time.Now().UTC().Add(-1 * time.Hour),
		isActive:        true,
	})
	neighborWorkerId := insertReassignTestWorker(t, pool, neighborTenantId, reassignTestWorkerOpts{
		lastHeartbeatAt: time.Now().UTC().Add(-1 * time.Hour),
		isActive:        true,
	})

	ownTask := insertReassignTestQueuedTask(t, pool, repo, tenantId, &ownWorkerId)
	neighborTask := insertReassignTestQueuedTask(t, pool, repo, neighborTenantId, &neighborWorkerId)

	_, _, err := repo.ProcessTaskReassignments(ctx(t), tenantId)
	require.NoError(t, err)

	ownItem := getReassignTestQueueItem(t, pool, ownTask, 0)
	require.NotNil(t, ownItem)
	assert.Nil(t, ownItem.DesiredWorkerID, "the scanned tenant's affinity must be cleared")

	neighborItem := getReassignTestQueueItem(t, pool, neighborTask, 0)
	require.NotNil(t, neighborItem, "the other tenant's queue item must survive")
	require.NotNil(t, neighborItem.DesiredWorkerID,
		"another tenant's affinity must not be cleared by this tenant's scan")
	assert.Equal(t, neighborWorkerId, *neighborItem.DesiredWorkerID)
}

// TestProcessTaskReassignmentsClearsDesiredWorkerAcrossRetryCounts verifies
// that the scan clears every queue item of the task that still points at the
// dead worker, not only the item for the task's current retry count, and that
// the task is reported exactly once.
func TestProcessTaskReassignmentsClearsDesiredWorkerAcrossRetryCounts(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createReassignTestTaskRepository(pool)
	tenantId := insertReassignTestTenant(t, pool, "reassign-multi-retry")

	staleWorkerId := insertReassignTestWorker(t, pool, tenantId, reassignTestWorkerOpts{
		lastHeartbeatAt: time.Now().UTC().Add(-1 * time.Hour),
		isActive:        true,
	})

	task := insertReassignTestQueuedTask(t, pool, repo, tenantId, &staleWorkerId)

	// Advance the task's retry count the way an internal failure does. The
	// update trigger requeues the task at the new retry count (copying the dead
	// affinity from v1_task), while the original queue item is left behind
	// because the task was never unqueued by a scheduler.
	_, err := pool.Exec(ctx(t), `UPDATE v1_task SET retry_count = 1 WHERE id = $1 AND inserted_at = $2`, task.ID, task.InsertedAt.Time)
	require.NoError(t, err)

	requeuedByTrigger := getReassignTestQueueItem(t, pool, task, 1)
	require.NotNil(t, requeuedByTrigger, "the update trigger must have requeued the task at retry count 1")
	require.NotNil(t, requeuedByTrigger.DesiredWorkerID,
		"the trigger copies v1_task.desired_worker_id into the new queue item")

	res, _, err := repo.ProcessTaskReassignments(ctx(t), tenantId)
	require.NoError(t, err)
	assert.Len(t, res.ClearedDesiredWorkerTasks, 1, "the task must be reported exactly once")

	for _, retryCount := range []int32{0, 1} {
		item := getReassignTestQueueItem(t, pool, task, retryCount)
		require.NotNil(t, item, "queue item for retry count %d must still exist", retryCount)
		assert.Nil(t, item.DesiredWorkerID,
			"queue item for retry count %d must lose the dead affinity", retryCount)
	}

	desiredWorkerID, _ := reassignTestTaskColumns(t, pool, task)
	assert.Nil(t, desiredWorkerID, "v1_task.desired_worker_id must be cleared")
}
