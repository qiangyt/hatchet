//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReapTenantDataRemovesAllTenantRows verifies that reaping a soft-deleted
// tenant hard-deletes its rows from every table with a tenantId column and
// the tenant row itself (FK-cascaded tables go with it).
func TestReapTenantDataRemovesAllTenantRows(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	tenantId := insertSoftDeleteTestTenant(t, pool, "reap-target")
	workerId := insertSoftDeleteTestWorker(t, pool, tenantId, time.Now().UTC().Add(-48*time.Hour))

	err := tenantRepo.DeleteTenant(ctx(t), tenantId)
	require.NoError(t, err)

	err = tenantRepo.ReapTenantData(ctx(t), tenantId)
	require.NoError(t, err)

	assert.False(t, softDeleteTestWorkerExists(t, pool, workerId),
		"worker rows owned by the reaped tenant must be deleted (FK cascade)")

	var tenantCount int
	err = pool.QueryRow(ctx(t), `SELECT count(*) FROM "Tenant" WHERE "id" = $1`, tenantId).Scan(&tenantCount)
	require.NoError(t, err)
	assert.Zero(t, tenantCount, "the tenant row itself must be hard-deleted")

	// Every remaining table with a tenantId column must hold zero rows for
	// the reaped tenant. The table list is discovered dynamically, so new
	// tables are covered automatically.
	for _, table := range softDeleteTestTablesTenantIdColumn(t, pool) {
		var count int
		err := pool.QueryRow(ctx(t),
			`SELECT count(*) FROM `+quoteIdent(table)+` WHERE "tenantId" = $1`, tenantId).Scan(&count)
		require.NoError(t, err, "failed to check table %s", table)
		assert.Zero(t, count, "table %s must hold no rows for the reaped tenant", table)
	}
}

// TestListSoftDeletedTenantsBeforeFiltersByGracePeriod verifies that only
// soft-deleted tenants deleted before the cutoff are returned.
func TestListSoftDeletedTenantsBeforeFiltersByGracePeriod(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	oldTenantId := insertSoftDeleteTestTenant(t, pool, "reap-old")
	recentTenantId := insertSoftDeleteTestTenant(t, pool, "reap-recent")

	for _, tenantId := range []uuid.UUID{oldTenantId, recentTenantId} {
		require.NoError(t, tenantRepo.DeleteTenant(ctx(t), tenantId))
	}
	// Backdate the old tenant's deletion past a 30-day grace period.
	_, err := pool.Exec(ctx(t), `
		UPDATE "Tenant" SET "deletedAt" = NOW() - INTERVAL '31 days' WHERE "id" = $1
	`, oldTenantId)
	require.NoError(t, err)

	cutoff := time.Now().UTC().Add(-30 * 24 * time.Hour)
	tenants, err := tenantRepo.ListSoftDeletedTenantsBefore(ctx(t), cutoff, 100)
	require.NoError(t, err)

	ids := make(map[uuid.UUID]bool, len(tenants))
	for _, tenant := range tenants {
		ids[tenant.ID] = true
	}
	assert.True(t, ids[oldTenantId], "tenant soft-deleted 31 days ago is past the cutoff")
	assert.False(t, ids[recentTenantId], "recently soft-deleted tenant is within the grace period")
}

// softDeleteTestTablesTenantIdColumn returns all public-schema tables that
// carry a tenantId column (the Tenant table has no such column and is
// naturally excluded).
func softDeleteTestTablesTenantIdColumn(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()

	rows, err := pool.Query(ctx(t), `
		SELECT table_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND column_name = 'tenantId'
		ORDER BY table_name
	`)
	require.NoError(t, err)
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		require.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, tables, "the schema must contain tables with a tenantId column")
	return tables
}

// quoteIdent double-quotes a SQL identifier coming from the database catalog.
func quoteIdent(name string) string {
	return `"` + name + `"`
}

// insertSoftDeleteTestService inserts a Service row for the tenant. The
// Service table has no tenantId foreign key, so it exercises the explicit
// per-table delete path in ReapTenantData (unlike Worker, which cascades).
func insertSoftDeleteTestService(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID, label string) uuid.UUID {
	t.Helper()

	serviceId := uuid.New()
	_, err := pool.Exec(ctx(t), `
		INSERT INTO "Service" ("id", "name", "tenantId")
		VALUES ($1, $2, $3)
	`, serviceId, label+"-"+serviceId.String(), tenantId)
	require.NoError(t, err)

	return serviceId
}

func softDeleteTestServiceExists(t *testing.T, pool *pgxpool.Pool, serviceId uuid.UUID) bool {
	t.Helper()

	var exists bool
	err := pool.QueryRow(ctx(t), `SELECT EXISTS (SELECT 1 FROM "Service" WHERE "id" = $1)`, serviceId).Scan(&exists)
	require.NoError(t, err)

	return exists
}

// TestReapTenantDataIsolatesOtherTenants verifies that reaping one tenant
// deletes only that tenant's rows — both the foreign-key-cascaded rows
// (Worker) and the explicitly deleted rows from tables without a foreign
// key (Service) — and leaves every other tenant's rows untouched.
func TestReapTenantDataIsolatesOtherTenants(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	targetTenantId := insertSoftDeleteTestTenant(t, pool, "reap-isolation-target")
	neighborTenantId := insertSoftDeleteTestTenant(t, pool, "reap-isolation-neighbor")

	for _, tenantId := range []uuid.UUID{targetTenantId, neighborTenantId} {
		require.NoError(t, tenantRepo.DeleteTenant(ctx(t), tenantId))
	}

	targetWorkerId := insertSoftDeleteTestWorker(t, pool, targetTenantId, time.Now().UTC().Add(-48*time.Hour))
	neighborWorkerId := insertSoftDeleteTestWorker(t, pool, neighborTenantId, time.Now().UTC().Add(-48*time.Hour))
	targetServiceId := insertSoftDeleteTestService(t, pool, targetTenantId, "isolation-target")
	neighborServiceId := insertSoftDeleteTestService(t, pool, neighborTenantId, "isolation-neighbor")

	require.NoError(t, tenantRepo.ReapTenantData(ctx(t), targetTenantId))

	assert.False(t, softDeleteTestWorkerExists(t, pool, targetWorkerId),
		"cascaded rows of the reaped tenant are deleted")
	assert.False(t, softDeleteTestServiceExists(t, pool, targetServiceId),
		"non-cascaded rows of the reaped tenant are deleted")

	assert.True(t, softDeleteTestWorkerExists(t, pool, neighborWorkerId),
		"rows of other tenants must survive the reap")
	assert.True(t, softDeleteTestServiceExists(t, pool, neighborServiceId),
		"rows of other tenants must survive the reap")

	var neighborTenantCount int
	err := pool.QueryRow(ctx(t), `SELECT count(*) FROM "Tenant" WHERE "id" = $1`, neighborTenantId).Scan(&neighborTenantCount)
	require.NoError(t, err)
	assert.Equal(t, 1, neighborTenantCount, "the other tenant row itself must survive")
}

// TestReapTenantDataFailsWhenTenantMissing verifies that reaping a tenant
// that no longer exists fails loudly instead of reporting success.
func TestReapTenantDataFailsWhenTenantMissing(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	tenantId := insertSoftDeleteTestTenant(t, pool, "reap-missing")

	err := tenantRepo.ReapTenantData(ctx(t), tenantId)
	require.NoError(t, err, "first reap succeeds")

	err = tenantRepo.ReapTenantData(ctx(t), tenantId)
	require.Error(t, err, "reaping an already-reaped tenant must fail, not silently succeed")
}

// TestReapTenantDataRollsBackOnTableDeleteFailure verifies that a failure
// while deleting one tenant-owned table rolls the whole reap back, leaving
// the tenant and its rows intact. A trigger on the Service table injects
// the failure.
func TestReapTenantDataRollsBackOnTableDeleteFailure(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	_, err := pool.Exec(ctx(t), `
		CREATE OR REPLACE FUNCTION fail_service_delete() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'injected delete failure';
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER fail_service_delete_trigger
		BEFORE DELETE ON "Service"
		FOR EACH ROW EXECUTE FUNCTION fail_service_delete();
	`)
	require.NoError(t, err)

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	tenantId := insertSoftDeleteTestTenant(t, pool, "reap-rollback")
	workerId := insertSoftDeleteTestWorker(t, pool, tenantId, time.Now().UTC().Add(-48*time.Hour))
	serviceId := insertSoftDeleteTestService(t, pool, tenantId, "rollback")

	err = tenantRepo.ReapTenantData(ctx(t), tenantId)
	require.Error(t, err, "a per-table delete failure must fail the reap")
	assert.Contains(t, err.Error(), "Service", "the error names the failing table")

	var tenantCount int
	err = pool.QueryRow(ctx(t), `SELECT count(*) FROM "Tenant" WHERE "id" = $1`, tenantId).Scan(&tenantCount)
	require.NoError(t, err)
	assert.Equal(t, 1, tenantCount, "the tenant row survives the rolled-back reap")

	assert.True(t, softDeleteTestWorkerExists(t, pool, workerId),
		"rows deleted earlier in the transaction are restored by the rollback")
	assert.True(t, softDeleteTestServiceExists(t, pool, serviceId),
		"the row of the failing table is untouched")
}

// TestListSoftDeletedTenantsBeforeExcludesActiveTenants verifies that
// active tenants never appear in the reap enumeration.
func TestListSoftDeletedTenantsBeforeExcludesActiveTenants(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	activeTenantId := insertSoftDeleteTestTenant(t, pool, "reap-enum-active")
	deletedTenantId := insertSoftDeleteTestTenant(t, pool, "reap-enum-deleted")
	require.NoError(t, tenantRepo.DeleteTenant(ctx(t), deletedTenantId))

	tenants, err := tenantRepo.ListSoftDeletedTenantsBefore(ctx(t), time.Now().UTC(), 100)
	require.NoError(t, err)

	ids := make(map[uuid.UUID]bool, len(tenants))
	for _, tenant := range tenants {
		ids[tenant.ID] = true
	}
	assert.False(t, ids[activeTenantId], "active tenants must never be enumerated for reap")
	assert.True(t, ids[deletedTenantId], "soft-deleted tenants are enumerated")
}

// TestListSoftDeletedTenantsBeforeRespectsLimitAndOrdering verifies that
// the enumeration is capped at maxCount and returns the oldest deletions
// first, so a full batch signals that another round should run.
func TestListSoftDeletedTenantsBeforeRespectsLimitAndOrdering(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	// Backdate three tenants to distinct, ordered deletion times.
	for i, ageDays := range []int{31, 20, 10} {
		tenantId := insertSoftDeleteTestTenant(t, pool, fmt.Sprintf("reap-order-%d", i))
		require.NoError(t, tenantRepo.DeleteTenant(ctx(t), tenantId))
		_, err := pool.Exec(ctx(t), `
			UPDATE "Tenant" SET "deletedAt" = NOW() - make_interval(days => $1) WHERE "id" = $2
		`, ageDays, tenantId)
		require.NoError(t, err)
	}

	cutoff := time.Now().UTC().Add(-5 * 24 * time.Hour)
	tenants, err := tenantRepo.ListSoftDeletedTenantsBefore(ctx(t), cutoff, 2)
	require.NoError(t, err)

	require.Len(t, tenants, 2, "maxCount caps the batch size")
	assert.Equal(t, "reap-order-0", tenants[0].Name, "oldest deletion comes first")
	assert.Equal(t, "reap-order-1", tenants[1].Name, "second oldest comes second")
}

// TestListSoftDeletedTenantsBeforeCutoffBoundary verifies the strict-less
// semantics of the cutoff: a tenant deleted exactly at the cutoff is not
// past it, one deleted a moment earlier is.
func TestListSoftDeletedTenantsBeforeCutoffBoundary(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	beforeTenantId := insertSoftDeleteTestTenant(t, pool, "reap-boundary-before")
	afterTenantId := insertSoftDeleteTestTenant(t, pool, "reap-boundary-after")
	for _, tenantId := range []uuid.UUID{beforeTenantId, afterTenantId} {
		require.NoError(t, tenantRepo.DeleteTenant(ctx(t), tenantId))
	}
	_, err := pool.Exec(ctx(t), `
		UPDATE "Tenant" SET "deletedAt" = NOW() - INTERVAL '1 hour' WHERE "id" = $1
	`, beforeTenantId)
	require.NoError(t, err)

	// Cutoff sits between the two deletion timestamps.
	tenants, err := tenantRepo.ListSoftDeletedTenantsBefore(ctx(t), time.Now().UTC().Add(-30*time.Minute), 100)
	require.NoError(t, err)

	ids := make(map[uuid.UUID]bool, len(tenants))
	for _, tenant := range tenants {
		ids[tenant.ID] = true
	}
	assert.True(t, ids[beforeTenantId], "tenant deleted before the cutoff is past it")
	assert.False(t, ids[afterTenantId], "tenant deleted after the cutoff is within grace")
}

// TestListSoftDeletedTenantsBeforeRejectsInvalidMaxCount verifies that an
// out-of-range batch size fails instead of silently truncating.
func TestListSoftDeletedTenantsBeforeRejectsInvalidMaxCount(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	for _, maxCount := range []int{-1, math.MaxInt32 + 1} {
		_, err := tenantRepo.ListSoftDeletedTenantsBefore(ctx(t), time.Now().UTC(), maxCount)
		require.Error(t, err, "maxCount %d must be rejected", maxCount)
	}
}

// TestListSoftDeletedTenantsBeforeFailsWhenPoolClosed verifies that an
// enumeration failure propagates instead of being reported as an empty
// result, so the reap pass aborts rather than silently skipping tenants.
func TestListSoftDeletedTenantsBeforeFailsWhenPoolClosed(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantRepo := createSoftDeleteTestTenantRepository(pool)
	pool.Close()

	_, err := tenantRepo.ListSoftDeletedTenantsBefore(ctx(t), time.Now().UTC(), 100)
	require.Error(t, err, "a database failure must propagate, not look like an empty batch")
}

// TestReapTenantDataFailsWhenPoolClosed verifies that a failure opening the
// reap transaction propagates instead of reporting a successful reap.
func TestReapTenantDataFailsWhenPoolClosed(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantRepo := createSoftDeleteTestTenantRepository(pool)
	pool.Close()

	err := tenantRepo.ReapTenantData(ctx(t), uuid.New())
	require.Error(t, err, "a transaction failure must propagate, not look like a successful reap")
}

// TestReapTenantDataFailsWhenTenantDeleteBlocked verifies that a failure
// deleting the tenant row itself propagates and rolls the reap back. A
// trigger on the Tenant table injects the failure.
func TestReapTenantDataFailsWhenTenantDeleteBlocked(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	_, err := pool.Exec(ctx(t), `
		CREATE OR REPLACE FUNCTION fail_tenant_delete() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'injected tenant delete failure';
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER fail_tenant_delete_trigger
		BEFORE DELETE ON "Tenant"
		FOR EACH ROW EXECUTE FUNCTION fail_tenant_delete();
	`)
	require.NoError(t, err)

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	tenantId := insertSoftDeleteTestTenant(t, pool, "reap-tenant-delete-blocked")
	workerId := insertSoftDeleteTestWorker(t, pool, tenantId, time.Now().UTC().Add(-48*time.Hour))

	err = tenantRepo.ReapTenantData(ctx(t), tenantId)
	require.Error(t, err, "a tenant-row delete failure must fail the reap")
	assert.Contains(t, err.Error(), "tenant row", "the error names the failing step")

	assert.True(t, softDeleteTestWorkerExists(t, pool, workerId),
		"the rolled-back reap leaves the tenant's rows intact")
}

// TestTenantIdTablesFailsWhenPoolClosed verifies that a catalog query
// failure propagates instead of being reported as an empty table list,
// which would silently skip every table.
func TestTenantIdTablesFailsWhenPoolClosed(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	pool.Close()

	_, err := tenantIdTables(ctx(t), pool)
	require.Error(t, err, "a catalog failure must propagate, not look like an empty table list")
}
