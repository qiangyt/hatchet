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

// This file covers the lifecycle of data owned by soft-deleted tenants
// (tenants whose "deletedAt" is set via DeleteTenant).
//
// The tests here pin the semantics that workloads creating and deleting
// tenants at a high rate (for example, ephemeral per-test tenants) rely on:
//
//  1. Soft-deleted tenant rows are reaped after a grace period — covered by
//     tenant_reap_test.go (ReapTenantData / ListSoftDeletedTenantsBefore).
//  2. Soft-deleted tenants are invisible to every partition listing query,
//     so the engine never schedules or runs controller loops for them.
//  3. Soft-deleted tenants never participate in partition rebalancing.
//  4. Deleting a tenant sets deletedAt and renames the slug so the original
//     slug can be reused.

func createSoftDeleteTestTenantRepository(pool *pgxpool.Pool) *tenantRepository {
	logger := zerolog.Nop()
	shared := &sharedRepository{
		pool:    pool,
		ddlPool: pool,
		l:       &logger,
		queries: sqlcv1.New(),
	}
	return &tenantRepository{
		sharedRepository:     shared,
		defaultTenantVersion: sqlcv1.TenantMajorEngineVersionV1,
	}
}

// insertSoftDeleteTestTenant inserts a V1 tenant and returns its id. The
// partition columns are set by the caller via explicit updates when needed.
func insertSoftDeleteTestTenant(t *testing.T, pool *pgxpool.Pool, label string) uuid.UUID {
	t.Helper()

	tenantId := uuid.New()
	_, err := pool.Exec(ctx(t), `
		INSERT INTO "Tenant" ("id", "name", "slug", "version")
		VALUES ($1, $2, $3, 'V1')
	`, tenantId, label, label+"-"+tenantId.String())
	require.NoError(t, err)

	return tenantId
}

func insertSoftDeleteTestPartition(t *testing.T, pool *pgxpool.Pool, table string, partitionId string) {
	t.Helper()

	_, err := pool.Exec(ctx(t), `
		INSERT INTO "`+table+`" ("id", "lastHeartbeat")
		VALUES ($1, NOW())
	`, partitionId)
	require.NoError(t, err)
}

func insertSoftDeleteTestWorker(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID, lastHeartbeatAt time.Time) uuid.UUID {
	t.Helper()

	dispatcherId := uuid.New()
	_, err := pool.Exec(ctx(t), `
		INSERT INTO "Dispatcher" ("id") VALUES ($1)
	`, dispatcherId)
	require.NoError(t, err)

	workerId := uuid.New()
	_, err = pool.Exec(ctx(t), `
		INSERT INTO "Worker" ("id", "tenantId", "name", "dispatcherId", "lastHeartbeatAt")
		VALUES ($1, $2, $3, $4, $5)
	`, workerId, tenantId, "worker-"+workerId.String(), dispatcherId, lastHeartbeatAt)
	require.NoError(t, err)

	return workerId
}

func softDeleteTestWorkerExists(t *testing.T, pool *pgxpool.Pool, workerId uuid.UUID) bool {
	t.Helper()

	var exists bool
	err := pool.QueryRow(ctx(t), `SELECT EXISTS (SELECT 1 FROM "Worker" WHERE "id" = $1)`, workerId).Scan(&exists)
	require.NoError(t, err)

	return exists
}

func softDeleteTestTenantColumns(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID) (slug string, deletedAt *time.Time, schedulerPartitionId *string) {
	t.Helper()

	err := pool.QueryRow(ctx(t), `
		SELECT "slug", "deletedAt", "schedulerPartitionId" FROM "Tenant" WHERE "id" = $1
	`, tenantId).Scan(&slug, &deletedAt, &schedulerPartitionId)
	require.NoError(t, err)

	return slug, deletedAt, schedulerPartitionId
}

// TestRebalanceAllSchedulerPartitionsSkipsSoftDeletedTenants verifies that
// soft-deleted tenants are not rewritten by the startup rebalance. The
// soft-deleted tenant starts with no scheduler partition assignment; if the
// rebalance touches it, it gains an active partition id.
//
// The rebalance query filters out soft-deleted tenants.
func TestRebalanceAllSchedulerPartitionsSkipsSoftDeletedTenants(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	activePartitionIds := []string{"rebalance-test-scheduler-1", "rebalance-test-scheduler-2"}
	for _, partitionId := range activePartitionIds {
		insertSoftDeleteTestPartition(t, pool, "SchedulerPartition", partitionId)
	}

	activeTenantId := insertSoftDeleteTestTenant(t, pool, "rebalance-active")
	deletedTenantId := insertSoftDeleteTestTenant(t, pool, "rebalance-deleted")

	_, err := pool.Exec(ctx(t), `
		UPDATE "Tenant" SET "schedulerPartitionId" = $1 WHERE "id" = $2
	`, activePartitionIds[0], activeTenantId)
	require.NoError(t, err)

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	err = tenantRepo.DeleteTenant(ctx(t), deletedTenantId)
	require.NoError(t, err)

	err = tenantRepo.RebalanceAllSchedulerPartitions(ctx(t))
	require.NoError(t, err)

	_, _, activeTenantPartition := softDeleteTestTenantColumns(t, pool, activeTenantId)
	require.NotNil(t, activeTenantPartition, "active tenant should hold a scheduler partition assignment")
	assert.Contains(t, activePartitionIds, *activeTenantPartition,
		"active tenant should be assigned to one of the active scheduler partitions")

	_, _, deletedTenantPartition := softDeleteTestTenantColumns(t, pool, deletedTenantId)
	assert.Nil(t, deletedTenantPartition,
		"soft-deleted tenants must not be assigned scheduler partitions by the rebalance")
}

// TestRebalanceInactiveSchedulerPartitionsSkipsSoftDeletedTenants verifies
// that the periodic inactive-partition rebalance does not move soft-deleted
// tenants onto an active partition. The rebalance moves affected tenants off
// inactive partitions and then deletes the inactive partition rows, which
// nulls the partition column of any tenant still pointing at them via the
// ON DELETE SET NULL foreign key; the soft-deleted tenant may therefore end
// up unassigned, but it must never land on an active partition.
func TestRebalanceInactiveSchedulerPartitionsSkipsSoftDeletedTenants(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	// An active partition and an inactive (stale-heartbeat) partition.
	insertSoftDeleteTestPartition(t, pool, "SchedulerPartition", "rebalance-inactive-live")
	_, err := pool.Exec(ctx(t), `
		INSERT INTO "SchedulerPartition" ("id", "lastHeartbeat") VALUES ($1, NOW() - INTERVAL '10 minutes')
	`, "rebalance-inactive-dead")
	require.NoError(t, err)

	activeTenantId := insertSoftDeleteTestTenant(t, pool, "rebalance-inactive-active")
	deletedTenantId := insertSoftDeleteTestTenant(t, pool, "rebalance-inactive-deleted")

	// Point both tenants at the inactive partition, then soft-delete one.
	for _, tenantId := range []uuid.UUID{activeTenantId, deletedTenantId} {
		_, err := pool.Exec(ctx(t), `
			UPDATE "Tenant" SET "schedulerPartitionId" = $1 WHERE "id" = $2
		`, "rebalance-inactive-dead", tenantId)
		require.NoError(t, err)
	}

	tenantRepo := createSoftDeleteTestTenantRepository(pool)
	err = tenantRepo.DeleteTenant(ctx(t), deletedTenantId)
	require.NoError(t, err)

	err = tenantRepo.RebalanceInactiveSchedulerPartitions(ctx(t))
	require.NoError(t, err)

	_, _, activeTenantPartition := softDeleteTestTenantColumns(t, pool, activeTenantId)
	require.NotNil(t, activeTenantPartition, "active tenant should be moved off the inactive partition")
	assert.Equal(t, "rebalance-inactive-live", *activeTenantPartition,
		"active tenant should be reassigned to the active partition")

	_, _, deletedTenantPartition := softDeleteTestTenantColumns(t, pool, deletedTenantId)
	assert.NotEqual(t, "rebalance-inactive-live", derefOrEmpty(deletedTenantPartition),
		"soft-deleted tenants must not be moved onto an active partition by the rebalance")
}

// derefOrEmpty returns the string value of a *string, or "" when nil.
func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// rebalanceSkipsSoftDeletedTenant drives one rebalance variant end to end:
// an active partition and a stale-heartbeat partition exist, both tenants
// are assigned to the stale partition, one is soft-deleted, and the
// rebalance runs. It asserts that the soft-deleted tenant never lands on
// the active partition, while the active tenant does. When
// includesInternalTenant is set, the internal tenant must also be assigned
// (the controller-partition variants deliberately keep it).
func rebalanceSkipsSoftDeletedTenant(
	t *testing.T,
	partitionTable string,
	partitionColumn string,
	rebalance func(repo *tenantRepository) error,
	includesInternalTenant bool,
) {
	t.Helper()

	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	const (
		livePartitionId = "rebalance-variant-live"
		deadPartitionId = "rebalance-variant-dead"
	)

	insertSoftDeleteTestPartition(t, pool, partitionTable, livePartitionId)
	_, err := pool.Exec(ctx(t), `
		INSERT INTO "`+partitionTable+`" ("id", "lastHeartbeat") VALUES ($1, NOW() - INTERVAL '10 minutes')
	`, deadPartitionId)
	require.NoError(t, err)

	activeTenantId := insertSoftDeleteTestTenant(t, pool, "rebalance-variant-active")
	deletedTenantId := insertSoftDeleteTestTenant(t, pool, "rebalance-variant-deleted")

	for _, tenantId := range []uuid.UUID{activeTenantId, deletedTenantId} {
		_, err := pool.Exec(ctx(t), `
			UPDATE "Tenant" SET "`+partitionColumn+`" = $1 WHERE "id" = $2
		`, deadPartitionId, tenantId)
		require.NoError(t, err)
	}

	tenantRepo := createSoftDeleteTestTenantRepository(pool)
	err = tenantRepo.DeleteTenant(ctx(t), deletedTenantId)
	require.NoError(t, err)

	err = rebalance(tenantRepo)
	require.NoError(t, err)

	assertPartitionColumn(t, pool, activeTenantId, partitionColumn, livePartitionId,
		"the active tenant must be moved onto the active partition")

	assertPartitionNotColumn(t, pool, deletedTenantId, partitionColumn, livePartitionId,
		"the soft-deleted tenant must never be moved onto an active partition")

	if includesInternalTenant {
		var internalPartition *string
		err := pool.QueryRow(ctx(t), `
			SELECT "`+partitionColumn+`" FROM "Tenant" WHERE "slug" = 'internal'
		`).Scan(&internalPartition)
		require.NoError(t, err)
		require.NotNil(t, internalPartition, "the internal tenant must be rebalanced")
		assert.Equal(t, livePartitionId, *internalPartition,
			"the controller partition rebalance deliberately includes the internal tenant")
	}
}

func assertPartitionColumn(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID, column string, expected string, message string) {
	t.Helper()

	var actual *string
	err := pool.QueryRow(ctx(t), `SELECT "`+column+`" FROM "Tenant" WHERE "id" = $1`, tenantId).Scan(&actual)
	require.NoError(t, err)
	require.NotNil(t, actual, "tenant %s must hold a %s assignment", tenantId, column)
	assert.Equal(t, expected, *actual, message)
}

func assertPartitionNotColumn(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID, column string, notExpected string, message string) {
	t.Helper()

	var actual *string
	err := pool.QueryRow(ctx(t), `SELECT "`+column+`" FROM "Tenant" WHERE "id" = $1`, tenantId).Scan(&actual)
	require.NoError(t, err)
	if actual != nil {
		assert.NotEqual(t, notExpected, *actual, message)
	}
}

// TestRebalanceAllControllerPartitionsSkipsSoftDeletedTenants covers the
// startup controller-partition rebalance. The controller variant
// deliberately includes the internal tenant.
func TestRebalanceAllControllerPartitionsSkipsSoftDeletedTenants(t *testing.T) {
	rebalanceSkipsSoftDeletedTenant(
		t,
		"ControllerPartition",
		"controllerPartitionId",
		func(repo *tenantRepository) error { return repo.RebalanceAllControllerPartitions(ctx(t)) },
		true,
	)
}

// TestRebalanceInactiveControllerPartitionsSkipsSoftDeletedTenants covers
// the periodic controller-partition rebalance, which also includes the
// internal tenant.
func TestRebalanceInactiveControllerPartitionsSkipsSoftDeletedTenants(t *testing.T) {
	rebalanceSkipsSoftDeletedTenant(
		t,
		"ControllerPartition",
		"controllerPartitionId",
		func(repo *tenantRepository) error { return repo.RebalanceInactiveControllerPartitions(ctx(t)) },
		true,
	)
}

// TestRebalanceAllTenantWorkerPartitionsSkipsSoftDeletedTenants covers the
// startup tenant-worker-partition rebalance.
func TestRebalanceAllTenantWorkerPartitionsSkipsSoftDeletedTenants(t *testing.T) {
	rebalanceSkipsSoftDeletedTenant(
		t,
		"TenantWorkerPartition",
		"workerPartitionId",
		func(repo *tenantRepository) error { return repo.RebalanceAllTenantWorkerPartitions(ctx(t)) },
		false,
	)
}

// TestRebalanceInactiveTenantWorkerPartitionsSkipsSoftDeletedTenants covers
// the periodic tenant-worker-partition rebalance.
func TestRebalanceInactiveTenantWorkerPartitionsSkipsSoftDeletedTenants(t *testing.T) {
	rebalanceSkipsSoftDeletedTenant(
		t,
		"TenantWorkerPartition",
		"workerPartitionId",
		func(repo *tenantRepository) error { return repo.RebalanceInactiveTenantWorkerPartitions(ctx(t)) },
		false,
	)
}

// TestListTenantsByPartitionsExcludesSoftDeletedTenants pins the semantics
// that all three partition listing queries exclude soft-deleted tenants:
// the engine never schedules, dispatches, or runs controller loops for them.
// Any reap path for soft-deleted tenants must preserve this behavior.
func TestListTenantsByPartitionsExcludesSoftDeletedTenants(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	const (
		controllerPartitionId = "partition-test-controller"
		schedulerPartitionId  = "partition-test-scheduler"
		workerPartitionId     = "partition-test-worker"
	)

	insertSoftDeleteTestPartition(t, pool, "ControllerPartition", controllerPartitionId)
	insertSoftDeleteTestPartition(t, pool, "SchedulerPartition", schedulerPartitionId)
	insertSoftDeleteTestPartition(t, pool, "TenantWorkerPartition", workerPartitionId)

	activeTenantId := insertSoftDeleteTestTenant(t, pool, "partition-active")
	deletedTenantId := insertSoftDeleteTestTenant(t, pool, "partition-deleted")

	for _, tenantId := range []uuid.UUID{activeTenantId, deletedTenantId} {
		_, err := pool.Exec(ctx(t), `
			UPDATE "Tenant"
			SET "controllerPartitionId" = $1, "schedulerPartitionId" = $2, "workerPartitionId" = $3
			WHERE "id" = $4
		`, controllerPartitionId, schedulerPartitionId, workerPartitionId, tenantId)
		require.NoError(t, err)
	}

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	err := tenantRepo.DeleteTenant(ctx(t), deletedTenantId)
	require.NoError(t, err)

	controllerTenantIds, err := tenantRepo.ListTenantsByControllerPartition(ctx(t), controllerPartitionId)
	require.NoError(t, err)
	assert.NotContains(t, controllerTenantIds, deletedTenantId,
		"controller partition listing must exclude soft-deleted tenants")
	assert.Contains(t, controllerTenantIds, activeTenantId,
		"controller partition listing must include the active tenant")

	schedulerTenants, err := tenantRepo.ListTenantsBySchedulerPartition(ctx(t), schedulerPartitionId)
	require.NoError(t, err)
	assert.NotContains(t, tenantIds(schedulerTenants), deletedTenantId,
		"scheduler partition listing must exclude soft-deleted tenants")

	workerTenants, err := tenantRepo.ListTenantsByWorkerPartition(ctx(t), workerPartitionId)
	require.NoError(t, err)
	assert.NotContains(t, tenantIds(workerTenants), deletedTenantId,
		"worker partition listing must exclude soft-deleted tenants")
}

// TestDeleteTenantSoftDeletesAndRenamesSlug pins the deletion semantics:
// deleting a tenant marks it with "deletedAt" and renames the slug so the
// original slug can be reused. A reap path may remove soft-deleted rows
// after a grace period but must not change these two effects.
func TestDeleteTenantSoftDeletesAndRenamesSlug(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantId := insertSoftDeleteTestTenant(t, pool, "delete-semantics")

	tenantRepo := createSoftDeleteTestTenantRepository(pool)
	err := tenantRepo.DeleteTenant(ctx(t), tenantId)
	require.NoError(t, err)

	slug, deletedAt, _ := softDeleteTestTenantColumns(t, pool, tenantId)

	require.NotNil(t, deletedAt, "DeleteTenant must set deletedAt")
	assert.Contains(t, slug, "_deleted_", "DeleteTenant must rename the slug to free it for reuse")
}

// tenantIds extracts the ids from a tenant list result.
func tenantIds(tenants []*sqlcv1.Tenant) []uuid.UUID {
	ids := make([]uuid.UUID, len(tenants))
	for i, tenant := range tenants {
		ids[i] = tenant.ID
	}
	return ids
}
