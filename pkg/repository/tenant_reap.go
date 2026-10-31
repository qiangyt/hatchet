package repository

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
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
// tables with a tenant column, then deletes the tenant row itself.
// Tables whose tenant foreign key cascades from the tenant row delete are
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

	var slug string
	err = tx.QueryRow(ctx, `SELECT "slug" FROM "Tenant" WHERE "id" = $1`, tenantId).Scan(&slug)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("tenant %s no longer exists", tenantId)
	} else if err != nil {
		return fmt.Errorf("could not look up tenant for reap: %w", err)
	}

	// The internal tenant (and its soft-delete-renamed descendants) is
	// engine-critical: the OLAP controller's partition maintenance job resolves
	// it on every cycle, and losing the row stops partition creation silently.
	// The generic soft-delete path cannot produce this state (any UPDATE of the
	// internal row raises on prevent_internal_name_or_slug), but the reap is
	// the last line of defense, so refuse here too.
	if slug == "internal" || strings.HasPrefix(slug, "internal_deleted_") {
		return fmt.Errorf("refusing to reap the internal tenant (slug %q): partition maintenance depends on it", slug)
	}

	tables, err := tenantIdTablesInDeleteOrder(ctx, tx)
	if err != nil {
		return err
	}

	// One DELETE per table, through the tenant column that table actually
	// has. Partitioned parents are deleted once — PostgreSQL routes the
	// statement to every child partition, so the daily OLAP partitions are
	// never enumerated individually.
	for _, table := range tables {
		column, err := quoteTenantColumn(table.column)
		if err != nil {
			return fmt.Errorf("could not delete rows from %s: %w", table.name, err)
		}

		tag, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s = $1`, quoteTable(table.name), column), tenantId)
		if err != nil {
			return fmt.Errorf("could not delete rows from %s: %w", table.name, err)
		}

		r.l.Debug().Ctx(ctx).
			Str("tenant_id", tenantId.String()).
			Str("table", table.name).
			Int64("rows", tag.RowsAffected()).
			Msg("reaped tenant rows")
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

// tenantTable pairs a tenant-scoped table with the name of its actual tenant
// column. v0 tables spell the column "tenantId" (camelCase); the entire v1
// family spells it "tenant_id" (snake_case). The reap must delete through the
// column the table really has — matching only one spelling silently skipped
// every v1 table, which is how soft-deleted tenants accumulated orphan rows
// in v1_step_concurrency and the rest of the v1 family.
type tenantTable struct {
	name   string
	column string
}

// quoteTenantColumn quotes a tenant column name for SQL, refusing anything
// outside the two known spellings. The discovery query already constrains
// attname to that set; this guard keeps an unvalidated catalog value from
// reaching SQL if that query is ever edited.
func quoteTenantColumn(name string) (string, error) {
	switch name {
	case "tenantId", "tenant_id":
		return `"` + name + `"`, nil
	default:
		return "", fmt.Errorf("unexpected tenant column name %q", name)
	}
}

// tenantIdTablesInDeleteOrder lists public-schema tables that carry a tenant
// column, ordered so that referencing (child) tables come before the tables
// they reference (parents). The list comes from the database catalog, so it
// never goes stale.
//
// Both tenant-column spellings are matched: "tenantId" (v0) and "tenant_id"
// (the whole v1 family). Declarative-partitioned tables are included through
// their parent (relkind 'p') and their children are excluded
// (relispartition) — a DELETE on the parent is routed by PostgreSQL to every
// child partition, so enumerating children would emit one statement per daily
// partition. A table carrying both spellings enters the list once; the
// camelCase column wins, deterministically (min(attname)).
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
}) ([]tenantTable, error) {
	rows, err := q.Query(ctx, `
		WITH RECURSIVE
		tenant_tables AS (
		    SELECT c.attrelid AS oid, min(c.attname) AS tenant_column
		    FROM pg_attribute c
		    JOIN pg_class cls ON cls.oid = c.attrelid
		    WHERE cls.relnamespace = 'public'::regnamespace
		      AND cls.relkind IN ('r', 'p')
		      AND cls.relispartition = false
		      AND c.attname IN ('tenantId', 'tenant_id')
		    GROUP BY c.attrelid
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
		SELECT cls.relname, tt.tenant_column
		FROM depths
		JOIN pg_class cls ON cls.oid = depths.t
		JOIN tenant_tables tt ON tt.oid = depths.t
		GROUP BY cls.oid, cls.relname, tt.tenant_column, depths.t
		ORDER BY max(depths.depth) DESC, cls.relname ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("could not discover tenant tables: %w", err)
	}
	defer rows.Close()

	var tables []tenantTable
	for rows.Next() {
		var table tenantTable
		if err := rows.Scan(&table.name, &table.column); err != nil {
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
