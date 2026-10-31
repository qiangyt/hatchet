//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"fmt"
	"math"
	"strings"
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

	// Every remaining table with a tenant column must hold zero rows for
	// the reaped tenant. The table list is discovered dynamically, so new
	// tables are covered automatically.
	for _, table := range softDeleteTestTablesWithTenantColumn(t, pool) {
		var count int
		err := pool.QueryRow(ctx(t),
			`SELECT count(*) FROM `+quoteIdent(table.name)+` WHERE `+quoteIdent(table.column)+` = $1`, tenantId).Scan(&count)
		require.NoError(t, err, "failed to check table %s", table.name)
		assert.Zero(t, count, "table %s must hold no rows for the reaped tenant", table.name)
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

// softDeleteTestTablesWithTenantColumn returns all public-schema tables that
// carry a tenant column, paired with the column's actual name — both the v0
// camelCase "tenantId" and the v1 snake_case "tenant_id" spellings (the
// Tenant table has no such column and is naturally excluded). Partitioned
// parents are included and their children excluded, mirroring the reap's own
// discovery: a count through the parent reads every child partition.
func softDeleteTestTablesWithTenantColumn(t *testing.T, pool *pgxpool.Pool) []tenantTable {
	t.Helper()

	rows, err := pool.Query(ctx(t), `
		SELECT cls.relname, min(c.attname)
		FROM pg_attribute c
		JOIN pg_class cls ON cls.oid = c.attrelid
		WHERE cls.relnamespace = 'public'::regnamespace
		  AND cls.relkind IN ('r', 'p')
		  AND cls.relispartition = false
		  AND c.attname IN ('tenantId', 'tenant_id')
		GROUP BY cls.relname
		ORDER BY cls.relname
	`)
	require.NoError(t, err)
	defer rows.Close()

	var tables []tenantTable
	for rows.Next() {
		var table tenantTable
		require.NoError(t, rows.Scan(&table.name, &table.column))
		tables = append(tables, table)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, tables, "the schema must contain tables with a tenant column")
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

	_, err := tenantIdTablesInDeleteOrder(ctx(t), pool)
	require.Error(t, err, "a catalog failure must propagate, not look like an empty table list")
}

// TestTenantIdTablesInDeleteOrder verifies the discovery ordering on the real
// migrated schema: every referencing (child) table sorts strictly before the
// table it references via an enforcement-mode FK. Alphabetical order — the
// pre-fix behavior — violated this on Step→Action (RESTRICT) and several
// NO ACTION pairs, which is what made production reaps fail with
// Step_actionId_tenantId_fkey.
func TestTenantIdTablesInDeleteOrder(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tables, err := tenantIdTablesInDeleteOrder(ctx(t), pool)
	require.NoError(t, err)

	position := make(map[string]int, len(tables))
	for i, table := range tables {
		position[table.name] = i
	}

	// The authoritative pair set, derived from the live catalog (2026-09-30
	// meta DB): enforcement-mode FKs (RESTRICT/NO ACTION/SET DEFAULT) whose
	// BOTH sides are tenant-scoped tables. SET NULL pairs impose no delete
	// order (parent delete nulls the child column); pairs with a global side
	// (no tenantId — "Dispatcher"/"Ticker"/"WorkflowTriggerScheduledRef"/…)
	// are outside the reap's discovery set. The two RESTRICT pairs here are
	// exactly what alphabetical order violated in production.
	childBeforeParent := [][2]string{
		{"Step", "Action"},
		{"StepRateLimit", "RateLimit"},
	}
	for _, pair := range childBeforeParent {
		childPos, childOK := position[pair[0]]
		parentPos, parentOK := position[pair[1]]
		require.True(t, childOK, "child table %s must be discovered", pair[0])
		require.True(t, parentOK, "parent table %s must be discovered", pair[1])
		assert.Less(t, childPos, parentPos,
			"child %s (pos %d) must be deleted before parent %s (pos %d)",
			pair[0], childPos, pair[1], parentPos)
	}
}

// TestReapTenantDataDeletesChildrenBeforeParents is the end-to-end regression
// for the production reap failure: a parent table that sorts alphabetically
// before its child (RESTRICT FK) used to be deleted first, tripping the FK
// and rolling the whole reap back — the tenant stayed soft-deleted forever
// (68 corpses on the production meta DB). The synthetic pair mirrors the
// real Action/Step shape: "aa_reap_parent" sorts before "zz_reap_child".
func TestReapTenantDataDeletesChildrenBeforeParents(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	_, err := pool.Exec(ctx(t), `
		CREATE TABLE "aa_reap_parent" (
		    "id" UUID NOT NULL,
		    "tenantId" UUID NOT NULL,
		    CONSTRAINT "aa_reap_parent_pkey" PRIMARY KEY ("id"),
		    CONSTRAINT "aa_reap_parent_id_tenantId_key" UNIQUE ("id", "tenantId")
		)
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx(t), `DROP TABLE IF EXISTS "zz_reap_child"`)
		_, _ = pool.Exec(ctx(t), `DROP TABLE IF EXISTS "aa_reap_parent"`)
	})

	_, err = pool.Exec(ctx(t), `
		CREATE TABLE "zz_reap_child" (
		    "id" UUID NOT NULL,
		    "tenantId" UUID NOT NULL,
		    "parentId" UUID NOT NULL,
		    CONSTRAINT "zz_reap_child_pkey" PRIMARY KEY ("id"),
		    CONSTRAINT "zz_reap_child_parent_fkey" FOREIGN KEY ("parentId", "tenantId")
		        REFERENCES "aa_reap_parent" ("id", "tenantId") ON DELETE RESTRICT
		)
	`)
	require.NoError(t, err)

	tenantRepo := createSoftDeleteTestTenantRepository(pool)
	tenantId := insertSoftDeleteTestTenant(t, pool, "reap-order")

	parentId := uuid.New()
	_, err = pool.Exec(ctx(t),
		`INSERT INTO "aa_reap_parent" ("id", "tenantId") VALUES ($1, $2)`,
		parentId, tenantId)
	require.NoError(t, err)
	childId := uuid.New()
	_, err = pool.Exec(ctx(t),
		`INSERT INTO "zz_reap_child" ("id", "tenantId", "parentId") VALUES ($1, $2, $3)`,
		childId, tenantId, parentId)
	require.NoError(t, err)

	err = tenantRepo.DeleteTenant(ctx(t), tenantId)
	require.NoError(t, err)

	// Pre-fix this failed with a foreign-key violation on aa_reap_parent
	// (alphabetical order deleted the parent before the child).
	err = tenantRepo.ReapTenantData(ctx(t), tenantId)
	require.NoError(t, err)

	for _, table := range []string{"aa_reap_parent", "zz_reap_child"} {
		var count int
		err := pool.QueryRow(ctx(t),
			`SELECT count(*) FROM `+quoteTable(table)+` WHERE "tenantId" = $1`, tenantId).Scan(&count)
		require.NoError(t, err, "failed to check table %s", table)
		assert.Zero(t, count, "table %s must hold no rows for the reaped tenant", table)
	}
}

// TestTenantIdTablesDiscoversSnakeCaseTables is the regression test for the
// v1-family leak: every v1 table names its tenant column "tenant_id"
// (snake_case), while the discovery query only ever matched the v0 spelling
// "tenantId". The whole v1 family — v1_step_concurrency,
// v1_tenant_concurrency, v1_concurrency_slot, v1_task, … — was therefore
// invisible to the reap, and soft-deleted tenants accumulated orphan rows in
// those tables forever (v1_step_concurrency alone grew ~108-180 rows per full
// qc e2e round on the production meta DB). Discovery must report each table
// with its actual column name so the DELETE targets the right column.
func TestTenantIdTablesDiscoversSnakeCaseTables(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tables, err := tenantIdTablesInDeleteOrder(ctx(t), pool)
	require.NoError(t, err)

	byName := make(map[string]string, len(tables))
	for _, table := range tables {
		byName[table.name] = table.column
	}

	// Representative non-partitioned v1 tables (metadata that accumulates
	// orphans) and the two concurrency tables at the heart of the leak.
	for _, name := range []string{
		"v1_step_concurrency",
		"v1_tenant_concurrency",
		"v1_concurrency_slot",
		"v1_workflow_concurrency",
		"v1_workflow_concurrency_slot",
	} {
		column, ok := byName[name]
		require.True(t, ok, "v1 table %s carries a tenant_id column and must be discovered", name)
		assert.Equal(t, "tenant_id", column, "table %s must report its actual tenant column", name)
	}

	// v0 tables keep reporting the camelCase column.
	column, ok := byName["Worker"]
	require.True(t, ok, "v0 table Worker must still be discovered")
	assert.Equal(t, "tenantId", column, "v0 tables keep the camelCase tenant column")
}

// TestTenantIdTablesIncludesPartitionedParentsExcludesChildren pins the
// partition handling: declarative-partitioned parents (relkind 'p') carry a
// tenant column too and must be reaped, but exactly once — PostgreSQL routes a
// DELETE on the parent to every child partition, so emitting one DELETE per
// daily child would multiply the work by the partition count (hundreds of
// statements against the OLAP tables). Partition children must not enter the
// list at all.
func TestTenantIdTablesIncludesPartitionedParentsExcludesChildren(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	// A synthetic partitioned family mirroring the real v1 shape: one parent
	// with a snake_case tenant column, three daily children. The reap must see
	// the parent once and the children never.
	_, err := pool.Exec(ctx(t), `
		CREATE TABLE "zz_reap_partitioned" (
		    "id" UUID NOT NULL,
		    "tenant_id" UUID NOT NULL,
		    "inserted_at" TIMESTAMPTZ NOT NULL,
		    CONSTRAINT "zz_reap_partitioned_pkey" PRIMARY KEY ("id", "inserted_at")
		) PARTITION BY RANGE ("inserted_at")
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx(t), `DROP TABLE IF EXISTS "zz_reap_partitioned"`)
	})

	for _, month := range []string{"2026-10", "2026-11", "2026-12"} {
		_, err = pool.Exec(ctx(t), fmt.Sprintf(`
			CREATE TABLE "zz_reap_partitioned_%s" PARTITION OF "zz_reap_partitioned"
			FOR VALUES FROM ('%s-01') TO ('%s-01'::timestamptz + INTERVAL '1 month')
		`, month, month, month))
		require.NoError(t, err)
	}

	tables, err := tenantIdTablesInDeleteOrder(ctx(t), pool)
	require.NoError(t, err)

	seen := make(map[string]int, len(tables))
	for _, table := range tables {
		seen[table.name]++
	}

	// The synthetic parent is discovered exactly once, with its real column.
	assert.Equal(t, 1, seen["zz_reap_partitioned"],
		"a partitioned parent must be discovered exactly once")
	for _, child := range []string{
		"zz_reap_partitioned_2026-10",
		"zz_reap_partitioned_2026-11",
		"zz_reap_partitioned_2026-12",
	} {
		assert.Zero(t, seen[child],
			"partition child %s must not be discovered: the parent DELETE routes to it", child)
	}

	// Real partitioned v1 parents are discovered exactly once as well.
	for _, name := range []string{"v1_task", "v1_dag", "v1_events_olap", "v1_tasks_olap"} {
		assert.Equal(t, 1, seen[name], "real partitioned parent %s must be discovered exactly once", name)
	}

	// Belt and braces: nothing in the list may be a partition child of any
	// table, whatever it is named.
	rows, err := pool.Query(ctx(t), `
		SELECT relname FROM pg_class
		WHERE relnamespace = 'public'::regnamespace AND relispartition
	`)
	require.NoError(t, err)
	var children []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		children = append(children, name)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, children, "the migrated schema must contain partition children")
	for _, child := range children {
		assert.Zero(t, seen[child], "partition child %s must never enter the reap list", child)
	}
}

// TestTenantIdTablesDeduplicatesDualColumnTable pins the dedup rule: a table
// carrying both spellings must enter the list exactly once, and the column
// choice must be fixed rather than arbitrary. The real schema has no such
// table (verified against the migrated catalog), so this is a determinism
// guard for future migrations, not a description of current data.
func TestTenantIdTablesDeduplicatesDualColumnTable(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	_, err := pool.Exec(ctx(t), `
		CREATE TABLE "zz_reap_dual_column" (
		    "id" UUID NOT NULL,
		    "tenantId" UUID NOT NULL,
		    "tenant_id" UUID NOT NULL,
		    CONSTRAINT "zz_reap_dual_column_pkey" PRIMARY KEY ("id")
		)
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx(t), `DROP TABLE IF EXISTS "zz_reap_dual_column"`)
	})

	// Two consecutive discoveries must agree — the rule is deterministic, not
	// dependent on catalog scan order.
	var columns []string
	for i := 0; i < 2; i++ {
		tables, err := tenantIdTablesInDeleteOrder(ctx(t), pool)
		require.NoError(t, err)

		seen := 0
		for _, table := range tables {
			if table.name == "zz_reap_dual_column" {
				seen++
				columns = append(columns, table.column)
			}
		}
		require.Equal(t, 1, seen, "a dual-column table must enter the list exactly once (run %d)", i)
	}

	require.Len(t, columns, 2)
	assert.Equal(t, columns[0], columns[1], "the column choice must be deterministic across runs")
	assert.Equal(t, "tenantId", columns[0],
		"when both spellings exist the v0 column wins, deterministically")
}

// TestReapTenantDataDeletesSnakeCaseTenantRows is the end-to-end behavior
// test for the v1 family: a soft-deleted tenant's v1_step_concurrency,
// v1_tenant_concurrency and partitioned v1-family rows must be reaped, while
// an active tenant and a tenant that is soft-deleted but still awaiting its
// grace period keep every row. It also pins that the reap never touches
// identity sequences (v1_tenant_concurrency borrows v1_step_concurrency's
// sequence, so a reset would collide future inserts) and that the
// v1_step_concurrency → v1_tenant_concurrency foreign key is respected
// children-first.
func TestReapTenantDataDeletesSnakeCaseTenantRows(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	tenantRepo := createSoftDeleteTestTenantRepository(pool)

	reapedTenantId := insertSoftDeleteTestTenant(t, pool, "reap-v1-target")
	awaitingTenantId := insertSoftDeleteTestTenant(t, pool, "reap-v1-awaiting")
	activeTenantId := insertSoftDeleteTestTenant(t, pool, "reap-v1-active")

	// A partitioned v1-family table with daily children, mirroring the real
	// OLAP/v1_task shape: the reap must clear the reaped tenant's rows via a
	// single parent DELETE that PostgreSQL routes to every child.
	_, err := pool.Exec(ctx(t), `
		CREATE TABLE "zz_reap_v1_partitioned" (
		    "id" UUID NOT NULL,
		    "tenant_id" UUID NOT NULL,
		    "inserted_at" TIMESTAMPTZ NOT NULL,
		    CONSTRAINT "zz_reap_v1_partitioned_pkey" PRIMARY KEY ("id", "inserted_at")
		) PARTITION BY RANGE ("inserted_at")
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx(t), `DROP TABLE IF EXISTS "zz_reap_v1_partitioned"`)
	})
	for _, month := range []string{"2026-10", "2026-11"} {
		_, err = pool.Exec(ctx(t), fmt.Sprintf(`
			CREATE TABLE "zz_reap_v1_partitioned_%s" PARTITION OF "zz_reap_v1_partitioned"
			FOR VALUES FROM ('%s-01') TO ('%s-01'::timestamptz + INTERVAL '1 month')
		`, month, month, month))
		require.NoError(t, err)
	}
	// Two rows per tenant, one per partition, so a parent-only delete that
	// fails to route to children leaves visible residue.
	for _, tenantId := range []uuid.UUID{reapedTenantId, awaitingTenantId, activeTenantId} {
		for _, day := range []string{"2026-10-15", "2026-11-15"} {
			_, err = pool.Exec(ctx(t), `
				INSERT INTO "zz_reap_v1_partitioned" ("id", "tenant_id", "inserted_at")
				VALUES ($1, $2, $3)
			`, uuid.New(), tenantId, day)
			require.NoError(t, err)
		}
	}

	// v1_tenant_concurrency first: v1_step_concurrency references it via
	// tenant_strategy_id (ON DELETE NO ACTION), so the reap must delete the
	// step rows before the tenant-concurrency rows.
	for _, tenantId := range []uuid.UUID{reapedTenantId, awaitingTenantId, activeTenantId} {
		insertSoftDeleteTestV1TenantConcurrency(t, pool, tenantId, "strategy-"+tenantId.String())
		insertSoftDeleteTestV1StepConcurrency(t, pool, tenantId, nil)
	}
	// Wire the reaped tenant's step row to its tenant strategy, exercising the
	// children-first order across the new v1 tables.
	stepStrategyId := softDeleteTestV1StepConcurrencyStrategyId(t, pool, reapedTenantId)
	tenantStrategyId := softDeleteTestV1TenantConcurrencyId(t, pool, reapedTenantId)
	_, err = pool.Exec(ctx(t),
		`UPDATE v1_step_concurrency SET tenant_strategy_id = $1 WHERE id = $2`,
		tenantStrategyId, stepStrategyId)
	require.NoError(t, err)

	var sequenceBefore int64
	sequenceBefore = softDeleteTestIdentitySequenceValue(t, pool, "v1_step_concurrency", "id")

	require.NoError(t, tenantRepo.DeleteTenant(ctx(t), reapedTenantId))
	require.NoError(t, tenantRepo.DeleteTenant(ctx(t), awaitingTenantId))

	err = tenantRepo.ReapTenantData(ctx(t), reapedTenantId)
	require.NoError(t, err)

	// The reaped tenant's v1 rows are gone — including the partitioned family,
	// where the count reads through the parent (all children).
	for _, table := range []string{
		"v1_step_concurrency", "v1_tenant_concurrency", "zz_reap_v1_partitioned",
	} {
		var count int
		err = pool.QueryRow(ctx(t),
			`SELECT count(*) FROM `+quoteIdent(table)+` WHERE tenant_id = $1`, reapedTenantId).Scan(&count)
		require.NoError(t, err, "failed to check table %s", table)
		assert.Zero(t, count, "table %s must hold no rows for the reaped tenant", table)
	}

	// The soft-deleted-but-awaiting tenant keeps every row: its Tenant row
	// still exists, so its data is not an orphan yet.
	assert.Equal(t, 1, softDeleteTestV1RowCount(t, pool, "v1_step_concurrency", awaitingTenantId),
		"the awaiting tenant's step-concurrency row must survive")
	assert.Equal(t, 1, softDeleteTestV1RowCount(t, pool, "v1_tenant_concurrency", awaitingTenantId),
		"the awaiting tenant's tenant-concurrency row must survive")
	assert.Equal(t, 2, softDeleteTestV1RowCount(t, pool, "zz_reap_v1_partitioned", awaitingTenantId),
		"the awaiting tenant's partitioned rows (one per child) must survive")

	// The active tenant keeps every row.
	assert.Equal(t, 1, softDeleteTestV1RowCount(t, pool, "v1_step_concurrency", activeTenantId),
		"the active tenant's step-concurrency row must survive")
	assert.Equal(t, 1, softDeleteTestV1RowCount(t, pool, "v1_tenant_concurrency", activeTenantId),
		"the active tenant's tenant-concurrency row must survive")
	assert.Equal(t, 2, softDeleteTestV1RowCount(t, pool, "zz_reap_v1_partitioned", activeTenantId),
		"the active tenant's partitioned rows (one per child) must survive")

	var reapedTenantCount, awaitingTenantCount int
	require.NoError(t, pool.QueryRow(ctx(t), `SELECT count(*) FROM "Tenant" WHERE "id" = $1`, reapedTenantId).Scan(&reapedTenantCount))
	require.NoError(t, pool.QueryRow(ctx(t), `SELECT count(*) FROM "Tenant" WHERE "id" = $1`, awaitingTenantId).Scan(&awaitingTenantCount))
	assert.Zero(t, reapedTenantCount, "the reaped tenant row is hard-deleted")
	assert.Equal(t, 1, awaitingTenantCount, "the awaiting tenant row survives the reap")

	// The shared identity sequence is untouched: a fresh insert must draw an id
	// beyond the pre-reap position, proving no RESTART IDENTITY happened.
	insertSoftDeleteTestV1StepConcurrency(t, pool, activeTenantId, nil)
	sequenceAfter := softDeleteTestIdentitySequenceValue(t, pool, "v1_step_concurrency", "id")
	assert.Greater(t, sequenceAfter, sequenceBefore,
		"the identity sequence must advance past its pre-reap value, never reset")
}

// insertSoftDeleteTestV1TenantConcurrency inserts a tenant-scoped concurrency
// strategy row (v1 family, snake_case tenant column).
func insertSoftDeleteTestV1TenantConcurrency(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID, name string) {
	t.Helper()

	_, err := pool.Exec(ctx(t), `
		INSERT INTO v1_tenant_concurrency ("tenant_id", "name", "strategy", "expression", "max_concurrency")
		VALUES ($1, $2, 'GROUP_ROUND_ROBIN', '1', 1)
	`, tenantId, name)
	require.NoError(t, err)
}

// insertSoftDeleteTestV1StepConcurrency inserts a per-step concurrency row
// (v1 family, snake_case tenant column).
func insertSoftDeleteTestV1StepConcurrency(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID, tenantStrategyId *int64) {
	t.Helper()

	_, err := pool.Exec(ctx(t), `
		INSERT INTO v1_step_concurrency
		    ("parent_strategy_id", "workflow_id", "workflow_version_id", "step_id",
		     "strategy", "expression", "tenant_id", "max_concurrency", "tenant_strategy_id")
		VALUES (NULL, $1, $1, $2, 'GROUP_ROUND_ROBIN', '1', $3, 1, $4)
	`, uuid.New(), uuid.New(), tenantId, tenantStrategyId)
	require.NoError(t, err)
}

func softDeleteTestV1RowCount(t *testing.T, pool *pgxpool.Pool, table string, tenantId uuid.UUID) int {
	t.Helper()

	var count int
	err := pool.QueryRow(ctx(t),
		`SELECT count(*) FROM `+quoteIdent(table)+` WHERE tenant_id = $1`, tenantId).Scan(&count)
	require.NoError(t, err, "failed to count table %s", table)
	return count
}

func softDeleteTestV1TenantConcurrencyId(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx(t),
		`SELECT id FROM v1_tenant_concurrency WHERE tenant_id = $1`, tenantId).Scan(&id)
	require.NoError(t, err)
	return id
}

func softDeleteTestV1StepConcurrencyStrategyId(t *testing.T, pool *pgxpool.Pool, tenantId uuid.UUID) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx(t),
		`SELECT id FROM v1_step_concurrency WHERE tenant_id = $1`, tenantId).Scan(&id)
	require.NoError(t, err)
	return id
}

// softDeleteTestIdentitySequenceValue returns the current last_value of the
// sequence backing an identity column, so a test can prove the reap never
// reset it.
func softDeleteTestIdentitySequenceValue(t *testing.T, pool *pgxpool.Pool, table, column string) int64 {
	t.Helper()

	var sequenceName string
	err := pool.QueryRow(ctx(t),
		`SELECT pg_get_serial_sequence($1, $2)`, table, column).Scan(&sequenceName)
	require.NoError(t, err)
	require.NotEmpty(t, sequenceName, "table %s column %s must be backed by a sequence", table, column)

	// pg_get_serial_sequence returns a schema-qualified name; quote each part
	// separately so the dot stays a schema separator.
	schema, name, found := strings.Cut(sequenceName, ".")
	if !found {
		schema, name = "public", sequenceName
	}
	qualified := quoteIdent(schema) + "." + quoteIdent(name)

	var value int64
	err = pool.QueryRow(ctx(t), `SELECT last_value FROM `+qualified).Scan(&value)
	require.NoError(t, err)
	return value
}
