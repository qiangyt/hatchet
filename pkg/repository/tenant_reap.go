package repository

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlchelpers"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// ListSoftDeletedTenantsBefore returns soft-deleted tenants whose deletion
// timestamp is older than the given cutoff, ordered oldest first, up to
// maxCount. The soft-delete tenant reap task uses this to enumerate tenants
// whose grace period has elapsed.
func (r *tenantRepository) ListSoftDeletedTenantsBefore(ctx context.Context, before time.Time, maxCount int) ([]*sqlcv1.Tenant, error) {
	if maxCount < 0 || maxCount > math.MaxInt32 {
		return nil, fmt.Errorf("maxCount %d is out of range", maxCount)
	}

	return r.queries.ListSoftDeletedTenantsBefore(ctx, r.pool, sqlcv1.ListSoftDeletedTenantsBeforeParams{
		Before:   sqlchelpers.TimestamptzFromTime(before),
		MaxCount: int32(maxCount),
	})
}

// ReapTenantData hard-deletes every row owned by the tenant across all
// tables with a tenantId column, then deletes the tenant row itself.
// Tables whose tenantId foreign key cascades from the tenant row delete are
// covered by the final delete; the explicit per-table deletes cover the
// rest. The table list comes from the database catalog, so new tables are
// covered automatically; the list is ordered children-first over the FK
// graph (see tenantIdTablesInDeleteOrder — deletion-blocking FKs such as
// Step→Action ON DELETE RESTRICT make alphabetical order fail). The whole
// operation runs in a single transaction.
func (r *tenantRepository) ReapTenantData(ctx context.Context, tenantId uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("could not begin reap transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tables, err := tenantIdTablesInDeleteOrder(ctx, tx)
	if err != nil {
		return err
	}

	for _, table := range tables {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE "tenantId" = $1`, quoteTable(table)), tenantId); err != nil {
			return fmt.Errorf("could not delete rows from %s: %w", table, err)
		}
	}

	rows, err := r.queries.HardDeleteTenant(ctx, tx, tenantId)
	if err != nil {
		return fmt.Errorf("could not delete tenant row: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("tenant %s no longer exists", tenantId)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("could not commit reap transaction: %w", err)
	}

	return nil
}

// tenantIdTablesInDeleteOrder lists public-schema tables that carry a
// tenantId column, ordered so that referencing (child) tables come before
// the tables they reference (parents). The list comes from the database
// catalog, so it never goes stale.
//
// Why ordering matters: enforcement-mode FKs (ON DELETE RESTRICT checks
// immediately, NO ACTION checks at statement end) make a parent-table delete
// fail while child rows still exist. Plain alphabetical order breaks on the
// real schema — e.g. "Action" sorts before "Step", but Step→Action is
// RESTRICT, so every tenant with workflow history failed to reap
// (Step_actionId_tenantId_fkey).
//
// Ordering is computed as the longest FK-chain depth per table over the
// enforcement-mode FK graph (CASCADE/SET NULL edges impose no order;
// self-references don't constrain a single-statement full-table delete and
// are excluded). Children sit strictly deeper than their parents, so any
// delete order within equal depths is safe. The depth cap keeps the
// recursion terminating if the FK graph ever grows a cycle — the delete
// then fails loudly on the cycle's own constraint rather than looping.
//
// The graph join runs on relation OIDs, NOT on regclass::text names:
// regclass text output quoting differs across PostgreSQL versions (PG15
// emits bare names, PG18 emits quoted names such as `"Step"`), which made a
// name-based join silently match nothing and fall back to alphabetical
// order. OIDs are version-stable.
func tenantIdTablesInDeleteOrder(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}) ([]string, error) {
	rows, err := q.Query(ctx, `
		WITH RECURSIVE
		tenant_tables AS (
		    SELECT DISTINCT c.attrelid AS oid
		    FROM pg_attribute c
		    JOIN pg_class cls ON cls.oid = c.attrelid
		    WHERE cls.relnamespace = 'public'::regnamespace
		      AND cls.relkind = 'r'
		      AND c.attname = 'tenantId'
		),
		blocking_edges AS (
		    SELECT DISTINCT con.conrelid AS child,
		                    con.confrelid AS parent
		    FROM pg_constraint con
		    WHERE con.contype = 'f'
		      AND con.confdeltype NOT IN ('c', 's')
		      AND con.conrelid <> con.confrelid
		),
		depths AS (
		    SELECT tt.oid AS t, 0 AS depth FROM tenant_tables tt
		    UNION
		    SELECT e.child, d.depth + 1
		    FROM depths d
		    JOIN blocking_edges e ON e.parent = d.t
		    JOIN tenant_tables tt ON tt.oid = e.child
		    WHERE d.depth < 100
		)
		SELECT cls.relname
		FROM depths
		JOIN pg_class cls ON cls.oid = depths.t
		GROUP BY cls.oid, cls.relname, depths.t
		ORDER BY max(depths.depth) DESC, cls.relname ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("could not discover tenant tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("could not scan table name: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("could not iterate table names: %w", err)
	}
	return tables, nil
}

// quoteTable quotes a SQL identifier coming from the database catalog.
func quoteTable(name string) string {
	return `"` + name + `"`
}
