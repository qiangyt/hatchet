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
// covered automatically. The whole operation runs in a single transaction.
func (r *tenantRepository) ReapTenantData(ctx context.Context, tenantId uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("could not begin reap transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tables, err := tenantIdTables(ctx, tx)
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

// tenantIdTables lists public-schema tables that carry a tenantId column.
// The list comes from the database catalog, so it never goes stale.
func tenantIdTables(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}) ([]string, error) {
	rows, err := q.Query(ctx, `
		SELECT table_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND column_name = 'tenantId'
		ORDER BY table_name
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
