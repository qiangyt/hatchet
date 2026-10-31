package task

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	tasktypes "github.com/hatchet-dev/hatchet/internal/services/shared/tasktypes/v1"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// fakeReassignTasksRepository serves a canned reassignment response so the
// controller path can be exercised without a database.
type fakeReassignTasksRepository struct {
	v1.TaskRepository

	res            *v1.FailTasksResponse
	shouldContinue bool
	err            error
	calls          int
}

func (f *fakeReassignTasksRepository) ProcessTaskReassignments(_ context.Context, _ uuid.UUID) (*v1.FailTasksResponse, bool, error) {
	f.calls++
	return f.res, f.shouldContinue, f.err
}

type fakeReassignRepository struct {
	v1.Repository

	tasks *fakeReassignTasksRepository
}

func (f *fakeReassignRepository) Tasks() v1.TaskRepository {
	return f.tasks
}

func newReassignTestController(mq msgqueue.MessageQueue, res *v1.FailTasksResponse, shouldContinue bool) (*TasksControllerImpl, *fakeReassignTasksRepository) {
	logger := zerolog.Nop()
	tasks := &fakeReassignTasksRepository{res: res, shouldContinue: shouldContinue}

	return &TasksControllerImpl{
		mq:        mq,
		l:         &logger,
		repov1:    &fakeReassignRepository{tasks: tasks},
		pubBuffer: msgqueue.NewMQPubBuffer(mq),
	}, tasks
}

func clearedAffinityRow(taskId int64, retryCount int32) *sqlcv1.ClearDesiredWorkerIdsForDisconnectedWorkersRow {
	return &sqlcv1.ClearDesiredWorkerIdsForDisconnectedWorkersRow{
		ID:         taskId,
		InsertedAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
		RetryCount: retryCount,
	}
}

// waitForSends waits until the async pub buffer has flushed n messages to the
// message queue, failing the test if that never happens.
func waitForSends(t *testing.T, mq *fakeMQ, n int64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mq.sends.Load() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.Failf(t, "timeout", "expected %d message(s) to be published, saw %d", n, mq.sends.Load())
}

func lastMonitoringEvent(t *testing.T, mq *fakeMQ) tasktypes.CreateMonitoringEventPayload {
	t.Helper()

	msg := mq.lastMsg.Load()
	require.NotNil(t, msg, "a message must have been published")
	require.NotEmpty(t, msg.Payloads, "the published message must carry a payload")

	var payload tasktypes.CreateMonitoringEventPayload
	require.NoError(t, json.Unmarshal(msg.Payloads[0], &payload))

	return payload
}

// TestProcessTaskReassignmentsPublishesClearedAffinityEvents pins the second
// tier's observability contract: every task whose desired-worker affinity was
// released because the worker is gone produces one REASSIGNED monitoring event
// on the OLAP queue, carrying the task id and the retry count of the released
// queue entry.
func TestProcessTaskReassignmentsPublishesClearedAffinityEvents(t *testing.T) {
	tenantId := uuid.New()
	mq := &fakeMQ{}

	c, tasks := newReassignTestController(mq, &v1.FailTasksResponse{
		FinalizedTaskResponse: &v1.FinalizedTaskResponse{},
		ClearedDesiredWorkerTasks: []*sqlcv1.ClearDesiredWorkerIdsForDisconnectedWorkersRow{
			clearedAffinityRow(42, 0),
			clearedAffinityRow(43, 2),
		},
	}, false)

	shouldContinue, err := c.processTaskReassignments(context.Background(), tenantId.String())
	require.NoError(t, err)
	assert.False(t, shouldContinue, "the repository's continuation signal must pass through")
	assert.Equal(t, 1, tasks.calls, "the reassignment pass must run exactly once")

	waitForSends(t, mq, 2)

	queue := mq.lastQueue.Load()
	require.NotNil(t, queue)
	assert.Equal(t, msgqueue.OLAP_QUEUE.Name(), *queue, "monitoring events belong on the OLAP queue")

	payload := lastMonitoringEvent(t, mq)
	assert.Equal(t, sqlcv1.V1EventTypeOlapREASSIGNED, payload.EventType)
	assert.Equal(t, tenantId, msgTenantID(t, mq))
	assert.Contains(t, payload.EventMessage, "desired worker",
		"the event message must state that the desired worker affinity was released")
	assert.Contains(t, payload.EventMessage, "another worker",
		"the event message must state the task can now be picked up elsewhere")
}

// TestProcessTaskReassignmentsPublishesNoEventsWithoutClearedAffinity
// verifies the negative case: a pass that only retried tasks (or did nothing)
// must not emit cleared-affinity events.
func TestProcessTaskReassignmentsPublishesNoEventsWithoutClearedAffinity(t *testing.T) {
	tenantId := uuid.New()
	mq := &fakeMQ{}

	c, _ := newReassignTestController(mq, &v1.FailTasksResponse{
		FinalizedTaskResponse: &v1.FinalizedTaskResponse{},
		RetriedTasks: []v1.RetriedTask{{
			TaskIdInsertedAtRetryCount: &v1.TaskIdInsertedAtRetryCount{Id: 7, RetryCount: 0},
		}},
	}, false)

	_, err := c.processTaskReassignments(context.Background(), tenantId.String())
	require.NoError(t, err)

	// The retried task is not an app error, so nothing is published at all.
	assert.Equal(t, int64(0), mq.sends.Load(), "no cleared affinity means no monitoring event")
}

// TestProcessTaskReassignmentsPropagatesRepositoryError verifies a repository
// failure surfaces instead of being reported as an empty pass.
func TestProcessTaskReassignmentsPropagatesRepositoryError(t *testing.T) {
	tenantId := uuid.New()
	mq := &fakeMQ{}

	c, _ := newReassignTestController(mq, nil, false)
	c.repov1.(*fakeReassignRepository).tasks.err = assert.AnError

	_, err := c.processTaskReassignments(context.Background(), tenantId.String())
	require.Error(t, err, "a repository failure must fail the pass")
	assert.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, int64(0), mq.sends.Load(), "a failed pass must not publish anything")
}

// TestProcessTaskReassignmentsPassesThroughContinuationSignal verifies the
// continuation flag from the repository reaches the caller untouched, since it
// drives whether the operation pool schedules another round.
func TestProcessTaskReassignmentsPassesThroughContinuationSignal(t *testing.T) {
	tenantId := uuid.New()
	mq := &fakeMQ{}

	c, _ := newReassignTestController(mq, &v1.FailTasksResponse{
		FinalizedTaskResponse: &v1.FinalizedTaskResponse{},
		ClearedDesiredWorkerTasks: []*sqlcv1.ClearDesiredWorkerIdsForDisconnectedWorkersRow{
			clearedAffinityRow(42, 0),
		},
	}, true)

	shouldContinue, err := c.processTaskReassignments(context.Background(), tenantId.String())
	require.NoError(t, err)
	assert.True(t, shouldContinue, "a batch that hit the limit must ask for another round")
}

func msgTenantID(t *testing.T, mq *fakeMQ) uuid.UUID {
	t.Helper()

	msg := mq.lastMsg.Load()
	require.NotNil(t, msg)

	return msg.TenantID
}
