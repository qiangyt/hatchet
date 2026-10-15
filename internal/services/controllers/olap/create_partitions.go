package olap

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/hatchet-dev/hatchet/internal/services/partition"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/hatchet-dev/hatchet/pkg/telemetry"
)

// internalTenantLookup narrows the partition service to the one call the
// partition cycle gates on, so runPartitionOnce is testable without a live
// partition service.
type internalTenantLookup interface {
	GetInternalTenantForController(ctx context.Context) (*sqlcv1.Tenant, error)
}

// compile-time assertion: the concrete partition service satisfies the seam.
var _ internalTenantLookup = (*partition.Partition)(nil)

func (oc *OLAPControllerImpl) runOLAPTablePartition(ctx context.Context) func() {
	return func() {
		runPartitionOnce(ctx, oc.l, oc.p, oc.repo.OLAP())
	}
}

func runPartitionOnce(ctx context.Context, l *zerolog.Logger, lookup internalTenantLookup, olapRepo v1.OLAPRepository) {
	l.Debug().Ctx(ctx).Msg("partition: running task table partition")

	// list all tenants
	tenant, err := lookup.GetInternalTenantForController(ctx)

	if err != nil {
		l.Error().Ctx(ctx).Err(err).Msg("could not get internal tenant")
		return
	}

	if tenant == nil {
		// The internal tenant row is missing (soft-deleted and reaped, or never
		// seeded). Partition DDL is system-wide and does not depend on the row —
		// returning here used to kill partition maintenance silently: no log at
		// any level, boot-time partitions masked the dead job for up to ~48h,
		// then every REST read against the OLAP tables failed fleet-wide with
		// "no partition of relation found" (SQLSTATE 23514). Log loudly and keep
		// the DDL running; the partition lease arbitrates concurrent instances.
		l.Error().Ctx(ctx).Msg("partition: internal tenant is missing from the database; continuing partition maintenance (the DDL does not depend on the row), but the engine is in a corrupted state — re-insert the internal tenant row (migration 20240216133745_v0_11_0 seeds it with fixed uuid 8d420720-ef03-41dc-9c73-1c93f276db97; the prevent_internal_name_or_slug trigger must be disabled for the re-insert)")
	}

	err = olapRepo.UpdateTablePartitions(ctx)

	if err != nil {
		if errors.Is(err, v1.ErrPartitionLockConflict) {
			l.Warn().Ctx(ctx).Msg("partition: lock conflict with concurrent table operation (e.g. ANALYZE), will retry at next interval")
			return
		}
		l.Error().Ctx(ctx).Err(err).Msg("could not create table partition")
	}
}

func (oc *OLAPControllerImpl) createTablePartition(ctx context.Context) error {
	ctx, span := telemetry.NewSpan(ctx, "OLAPControllerImpl.createTablePartition")
	defer span.End()

	err := oc.repo.OLAP().UpdateTablePartitions(ctx)

	if err != nil {
		return fmt.Errorf("could not create table partition: %w", err)
	}

	return nil
}
