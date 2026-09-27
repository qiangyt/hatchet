package retention

import (
	"context"
	"time"
)

// reapBatchSize bounds how many soft-deleted tenants are loaded per query
// round; when a full batch is reaped, another round runs in the same pass.
const reapBatchSize = 100

// runReapSoftDeletedTenants hard-deletes soft-deleted tenants whose grace
// period has elapsed. It loads tenants deleted before now minus the
// configured grace duration in batches and reaps each one; a failing tenant
// is logged and skipped so the rest of the batch still runs.
func (rc *RetentionControllerImpl) runReapSoftDeletedTenants(ctx context.Context) func() {
	return func() {
		rc.l.Debug().Msg("retention controller: reaping soft-deleted tenants")

		cutoff := time.Now().UTC().Add(-rc.softDeleteTenantReapGrace)

		for {
			tenants, err := rc.repo.Tenant().ListSoftDeletedTenantsBefore(ctx, cutoff, reapBatchSize)
			if err != nil {
				rc.l.Error().Err(err).Msg("could not list soft-deleted tenants for reap")
				return
			}

			for _, tenant := range tenants {
				if err := rc.repo.Tenant().ReapTenantData(ctx, tenant.ID); err != nil {
					rc.l.Error().Err(err).Msgf("could not reap soft-deleted tenant %s", tenant.ID)
					continue
				}
				rc.l.Info().Msgf("reaped soft-deleted tenant %s", tenant.ID)
			}

			if len(tenants) < reapBatchSize {
				return
			}
		}
	}
}
