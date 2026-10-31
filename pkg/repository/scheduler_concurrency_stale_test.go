//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// This file pins the semantics of DeactivateStaleStepConcurrency: a strategy
// (step-scoped or tenant-scoped) goes inactive only when it is idle for longer
// than the caller-supplied threshold AND holds no concurrency slots. The
// threshold is a parameter so operators can retire stale strategies faster
// than the 25h engine default on shared instances.

func createStaleDeactivationTestRepository(pool *pgxpool.Pool) *ConcurrencyRepositoryImpl {
	logger := zerolog.Nop()
	shared := &sharedRepository{
		pool:    pool,
		ddlPool: pool,
		l:       &logger,
		queries: sqlcv1.New(),
	}
	return &ConcurrencyRepositoryImpl{sharedRepository: shared}
}

// insertStaleStepStrategy inserts an active step strategy whose last_active_at
// is backdated by lastActiveAgo and returns its id.
func insertStaleStepStrategy(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID, lastActiveAgo time.Duration) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx(t), `
		INSERT INTO v1_step_concurrency (
			workflow_id, workflow_version_id, step_id, tenant_id,
			strategy, expression, max_concurrency, last_active_at
		) VALUES ($1, $2, $3, $4, 'CANCEL_IN_PROGRESS', '1', 1, $5)
		RETURNING id
	`, uuid.New(), uuid.New(), uuid.New(), tenantId, time.Now().UTC().Add(-lastActiveAgo)).Scan(&id)
	require.NoError(t, err)

	return id
}

// insertStaleTenantStrategy inserts an active tenant-scoped strategy whose
// last_active_at is backdated by lastActiveAgo and returns its id.
func insertStaleTenantStrategy(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID, name string, lastActiveAgo time.Duration) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx(t), `
		INSERT INTO v1_tenant_concurrency (
			tenant_id, name, strategy, expression, max_concurrency, last_active_at
		) VALUES ($1, $2, 'CANCEL_IN_PROGRESS', '1', 1, $3)
		RETURNING id
	`, tenantId, name, time.Now().UTC().Add(-lastActiveAgo)).Scan(&id)
	require.NoError(t, err)

	return id
}

// insertStaleStrategySlot inserts one concurrency slot referencing strategyId,
// then re-backdates the strategy's last_active_at: the slot-insert trigger
// bumps last_active_at to NOW() whenever it is older than an hour, which would
// otherwise mask the backdating done at insert time.
func insertStaleStrategySlot(t *testing.T, pool *pgxpool.Pool, table string, tenantId uuid.UUID, strategyId int64, lastActiveAgo time.Duration) {
	t.Helper()

	_, err := pool.Exec(ctx(t), `
		INSERT INTO v1_concurrency_slot (
			task_id, task_inserted_at, task_retry_count, external_id, tenant_id,
			workflow_id, workflow_version_id, workflow_run_id, strategy_id,
			key, queue_to_notify, schedule_timeout_at
		) VALUES ($1, $2, 0, $3, $4, $5, $6, $7, $8, 'stale-test', 'stale-test', NOW() + INTERVAL '1 hour')
	`, int64(1), time.Now().UTC(), uuid.New(), tenantId, uuid.New(), uuid.New(), uuid.New(), strategyId)
	require.NoError(t, err)

	_, err = pool.Exec(ctx(t), `
		UPDATE `+table+` SET last_active_at = $1 WHERE id = $2
	`, time.Now().UTC().Add(-lastActiveAgo), strategyId)
	require.NoError(t, err)
}

func stepStrategyIsActive(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID, strategyId int64) bool {
	t.Helper()

	var isActive bool
	err := pool.QueryRow(ctx(t), `
		SELECT is_active FROM v1_step_concurrency WHERE tenant_id = $1 AND id = $2
	`, tenantId, strategyId).Scan(&isActive)
	require.NoError(t, err)

	return isActive
}

func tenantStrategyIsActive(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID, strategyId int64) bool {
	t.Helper()

	var isActive bool
	err := pool.QueryRow(ctx(t), `
		SELECT is_active FROM v1_tenant_concurrency WHERE tenant_id = $1 AND id = $2
	`, tenantId, strategyId).Scan(&isActive)
	require.NoError(t, err)

	return isActive
}

// TestDeactivateStaleStepConcurrencyDeactivatesIdleStrategy verifies that a
// step strategy idle beyond the threshold with no slots is deactivated.
func TestDeactivateStaleStepConcurrencyDeactivatesIdleStrategy(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createStaleDeactivationTestRepository(pool)

	tenantId := uuid.New()
	strategyId := insertStaleStepStrategy(t, pool, tenantId, 3*time.Hour)

	err := repo.DeactivateStaleStepConcurrency(ctx(t), tenantId, 2*time.Hour)
	require.NoError(t, err)

	assert.False(t, stepStrategyIsActive(t, pool, tenantId, strategyId),
		"a strategy idle 3h under a 2h threshold with no slots must be deactivated")
}

// TestDeactivateStaleStepConcurrencyKeepsStrategyWithSlots verifies that a
// slot still referencing the strategy blocks deactivation even when the
// strategy is idle beyond the threshold.
func TestDeactivateStaleStepConcurrencyKeepsStrategyWithSlots(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createStaleDeactivationTestRepository(pool)

	tenantId := uuid.New()
	strategyId := insertStaleStepStrategy(t, pool, tenantId, 3*time.Hour)
	insertStaleStrategySlot(t, pool, "v1_step_concurrency", tenantId, strategyId, 3*time.Hour)

	err := repo.DeactivateStaleStepConcurrency(ctx(t), tenantId, 2*time.Hour)
	require.NoError(t, err)

	assert.True(t, stepStrategyIsActive(t, pool, tenantId, strategyId),
		"a strategy still referenced by a concurrency slot must stay active")
}

// TestDeactivateStaleStepConcurrencyKeepsFreshStrategy verifies that a
// strategy active within the threshold is not deactivated.
func TestDeactivateStaleStepConcurrencyKeepsFreshStrategy(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createStaleDeactivationTestRepository(pool)

	tenantId := uuid.New()
	strategyId := insertStaleStepStrategy(t, pool, tenantId, 1*time.Hour)

	err := repo.DeactivateStaleStepConcurrency(ctx(t), tenantId, 2*time.Hour)
	require.NoError(t, err)

	assert.True(t, stepStrategyIsActive(t, pool, tenantId, strategyId),
		"a strategy active 1h ago under a 2h threshold must stay active")
}

// TestDeactivateStaleConcurrencyThresholdIsConfigurable pins the reason the
// threshold is a parameter: the same backdated strategy must deactivate under
// a short threshold while surviving the 25h engine default.
func TestDeactivateStaleConcurrencyThresholdIsConfigurable(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createStaleDeactivationTestRepository(pool)

	tenantId := uuid.New()
	recentId := insertStaleStepStrategy(t, pool, tenantId, 3*time.Hour)
	ancientId := insertStaleStepStrategy(t, pool, tenantId, 30*time.Hour)

	// 25h (the engine default): only the 30h-old strategy is past the cutoff.
	err := repo.DeactivateStaleStepConcurrency(ctx(t), tenantId, 25*time.Hour)
	require.NoError(t, err)

	assert.True(t, stepStrategyIsActive(t, pool, tenantId, recentId),
		"a 3h-idle strategy survives the 25h default threshold")
	assert.False(t, stepStrategyIsActive(t, pool, tenantId, ancientId),
		"a 30h-idle strategy is deactivated under the 25h default threshold")

	// 2h (an operator-supplied threshold): the 3h-old strategy now deactivates.
	err = repo.DeactivateStaleStepConcurrency(ctx(t), tenantId, 2*time.Hour)
	require.NoError(t, err)

	assert.False(t, stepStrategyIsActive(t, pool, tenantId, recentId),
		"a 3h-idle strategy deactivates once the threshold is lowered to 2h")
}

// TestDeactivateStaleTenantConcurrencyDeactivatesIdleStrategy covers the
// tenant-scoped twin query: idle beyond the threshold and slotless deactivates,
// fresh or slot-referenced strategies survive.
func TestDeactivateStaleTenantConcurrencyDeactivatesIdleStrategy(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createStaleDeactivationTestRepository(pool)

	tenantId := uuid.New()
	idleId := insertStaleTenantStrategy(t, pool, tenantId, "stale-idle", 3*time.Hour)
	freshId := insertStaleTenantStrategy(t, pool, tenantId, "stale-fresh", 1*time.Hour)
	slottedId := insertStaleTenantStrategy(t, pool, tenantId, "stale-slotted", 3*time.Hour)
	insertStaleStrategySlot(t, pool, "v1_tenant_concurrency", tenantId, slottedId, 3*time.Hour)

	err := repo.DeactivateStaleStepConcurrency(ctx(t), tenantId, 2*time.Hour)
	require.NoError(t, err)

	assert.False(t, tenantStrategyIsActive(t, pool, tenantId, idleId),
		"a tenant strategy idle 3h under a 2h threshold with no slots must be deactivated")
	assert.True(t, tenantStrategyIsActive(t, pool, tenantId, freshId),
		"a tenant strategy active 1h ago under a 2h threshold must stay active")
	assert.True(t, tenantStrategyIsActive(t, pool, tenantId, slottedId),
		"a tenant strategy still referenced by a concurrency slot must stay active")
}
