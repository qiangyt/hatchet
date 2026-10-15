//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The internal tenant is seeded by migration 20240216133745_v0_11_0 with a
// fixed uuid and is load-bearing engine infrastructure: the OLAP controller's
// partition maintenance job resolves it on every cycle. In production the row
// vanished (hard-deleted by a path outside the engine — any UPDATE of the row
// raises on prevent_internal_name_or_slug, so it cannot be soft-deleted and
// therefore cannot reach the reap job), partition maintenance silently stopped,
// and every REST read against the OLAP tables died once the boot-time
// partitions ran out. These tests pin three layers of protection:
// repository-level refusal on the two deletion entry points, and a database
// trigger closing the bare-DELETE gap.

// internalTenantId (the migration-seeded engine-critical row) is defined in
// workflow_cron_cleanup_test.go.

func TestReapTenantData_RefusesInternalTenant(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createSoftDeleteTestTenantRepository(pool)

	var slug string
	err := pool.QueryRow(ctx(t), `SELECT "slug" FROM "Tenant" WHERE "id" = $1`, internalTenantId).Scan(&slug)
	require.NoError(t, err, "the migration-seeded internal tenant row must exist in a migrated database")
	require.Equal(t, "internal", slug)

	err = repo.ReapTenantData(ctx(t), internalTenantId)

	require.Error(t, err, "reaping the internal tenant must be refused")
	assert.Contains(t, err.Error(), "internal tenant")

	var count int
	err = pool.QueryRow(ctx(t), `SELECT count(*) FROM "Tenant" WHERE "id" = $1`, internalTenantId).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "the internal tenant row must survive the refused reap")
}

func TestReapTenantData_RefusesRenamedInternalTenant(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createSoftDeleteTestTenantRepository(pool)

	// A soft-deleted internal tenant carries the DeleteTenant slug rename
	// (slug || '_deleted_' || uuid); the reap must recognize the family and
	// refuse, not just the pristine 'internal' slug. The soft-delete state is
	// produced with raw SQL because DeleteTenant itself now refuses the
	// internal family — the row models a legacy state created before the
	// repository guards existed.
	tenantId := insertSoftDeleteTestTenant(t, pool, "internal_deleted_seed")
	_, err := pool.Exec(ctx(t),
		`UPDATE "Tenant" SET "deletedAt" = NOW(), "slug" = "slug" || '_deleted_' || gen_random_uuid() WHERE "id" = $1`,
		tenantId)
	require.NoError(t, err)

	err = repo.ReapTenantData(ctx(t), tenantId)

	require.Error(t, err, "reaping a renamed internal tenant must be refused")
	assert.Contains(t, err.Error(), "internal tenant")

	var count int
	err = pool.QueryRow(ctx(t), `SELECT count(*) FROM "Tenant" WHERE "id" = $1`, tenantId).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "the renamed internal tenant row must survive the refused reap")
}

func TestDeleteTenant_RefusesInternalTenant(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createSoftDeleteTestTenantRepository(pool)

	err := repo.DeleteTenant(ctx(t), internalTenantId)

	require.Error(t, err, "soft-deleting the internal tenant must be refused by the repository, not left to an incidental constraint violation")
	assert.Contains(t, err.Error(), "internal tenant")

	var deletedAtCount int
	err = pool.QueryRow(ctx(t), `SELECT count(*) FROM "Tenant" WHERE "id" = $1 AND "deletedAt" IS NULL`, internalTenantId).Scan(&deletedAtCount)
	require.NoError(t, err)
	assert.Equal(t, 1, deletedAtCount, "the internal tenant row must remain live after the refused delete")
}

func TestInternalTenantDeletionBlockedAtDatabaseLevel(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	internalId := internalTenantId

	// The bare-DELETE gap: the legacy trigger only guards INSERT/UPDATE, so a
	// direct DELETE bypassed every in-engine guard and is the only remaining
	// explanation for the production incident. The database itself must refuse.
	_, err := pool.Exec(ctx(t), `DELETE FROM "Tenant" WHERE "id" = $1`, internalId)
	require.Error(t, err, "a bare DELETE of the internal tenant must be rejected by the database")
	assert.Contains(t, err.Error(), "internal tenant")

	// Soft-delete via raw UPDATE is already caught by the legacy name/slug
	// trigger (the row keeps name='internal'); keep it pinned so a future
	// trigger edit cannot silently drop either guard.
	_, err = pool.Exec(ctx(t), `UPDATE "Tenant" SET "deletedAt" = NOW() WHERE "id" = $1`, internalId)
	require.Error(t, err, "a raw soft-delete UPDATE of the internal tenant must be rejected by the database")

	// The guard must not over-block: normal tenants delete fine.
	normalId := insertSoftDeleteTestTenant(t, pool, "deletable")
	_, err = pool.Exec(ctx(t), `DELETE FROM "Tenant" WHERE "id" = $1`, normalId)
	require.NoError(t, err, "the deletion guard must only cover the internal tenant family")
}
