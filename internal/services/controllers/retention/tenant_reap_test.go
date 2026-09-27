//go:build !e2e && !load && !rampup && !integration

package retention

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"

	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

type fakeReapRepo struct {
	v1.Repository
	tenantRepo *fakeReapTenantRepo
}

func (f *fakeReapRepo) Tenant() v1.TenantRepository { return f.tenantRepo }

type fakeReapTenantRepo struct {
	v1.TenantRepository

	before       time.Time
	listed       []*sqlcv1.Tenant
	listedNext   [][]*sqlcv1.Tenant
	reaped       []uuid.UUID
	maxCountSeen int
	listErr      error
	reapErr      map[uuid.UUID]error
}

func (f *fakeReapTenantRepo) ListSoftDeletedTenantsBefore(ctx context.Context, before time.Time, maxCount int) ([]*sqlcv1.Tenant, error) {
	f.before = before
	f.maxCountSeen = maxCount
	if f.listErr != nil {
		return nil, f.listErr
	}
	if len(f.listedNext) > 0 {
		batch := f.listedNext[0]
		f.listedNext = f.listedNext[1:]
		return batch, nil
	}
	return f.listed, nil
}

func (f *fakeReapTenantRepo) ReapTenantData(ctx context.Context, tenantId uuid.UUID) error {
	if err, ok := f.reapErr[tenantId]; ok {
		return err
	}
	f.reaped = append(f.reaped, tenantId)
	return nil
}

func newReapTestController(repo v1.Repository, grace time.Duration) *RetentionControllerImpl {
	logger := zerolog.Nop()
	return &RetentionControllerImpl{
		l:                         &logger,
		repo:                      repo,
		softDeleteTenantReap:      true,
		softDeleteTenantReapGrace: grace,
	}
}

// TestRunReapSoftDeletedTenantsReapsPastGrace verifies that the reap task
// reaps every soft-deleted tenant past the grace period and computes the
// cutoff from the configured grace duration.
func TestRunReapSoftDeletedTenantsReapsPastGrace(t *testing.T) {
	tenantA := uuid.New()
	tenantB := uuid.New()

	tenantRepo := &fakeReapTenantRepo{listed: []*sqlcv1.Tenant{
		{ID: tenantA}, {ID: tenantB},
	}}
	rc := newReapTestController(&fakeReapRepo{tenantRepo: tenantRepo}, 30*24*time.Hour)

	start := time.Now().UTC()
	rc.runReapSoftDeletedTenants(context.Background())()

	assert.ElementsMatch(t, []uuid.UUID{tenantA, tenantB}, tenantRepo.reaped,
		"all listed tenants past grace are reaped")

	expectedCutoff := start.Add(-30 * 24 * time.Hour)
	assert.WithinDuration(t, expectedCutoff, tenantRepo.before, time.Minute,
		"cutoff equals now minus the configured grace period")
}

// TestRunReapSoftDeletedTenantsContinuesOnError verifies that a failing
// tenant does not abort the rest of the batch.
func TestRunReapSoftDeletedTenantsContinuesOnError(t *testing.T) {
	tenantA := uuid.New()
	tenantB := uuid.New()

	tenantRepo := &fakeReapTenantRepo{listed: []*sqlcv1.Tenant{
		{ID: tenantA}, {ID: tenantB},
	}, reapErr: map[uuid.UUID]error{tenantA: assert.AnError}}
	rc := newReapTestController(&fakeReapRepo{tenantRepo: tenantRepo}, time.Hour)

	rc.runReapSoftDeletedTenants(context.Background())()

	assert.ElementsMatch(t, []uuid.UUID{tenantB}, tenantRepo.reaped,
		"the healthy tenant is still reaped when another one fails")
}

// TestRunReapSoftDeletedTenantsLoopsFullBatches verifies that a full batch
// triggers another round in the same pass, so a backlog larger than one
// batch is fully drained before the task returns.
func TestRunReapSoftDeletedTenantsLoopsFullBatches(t *testing.T) {
	fullBatch := make([]*sqlcv1.Tenant, reapBatchSize)
	for i := range fullBatch {
		fullBatch[i] = &sqlcv1.Tenant{ID: uuid.New()}
	}
	remainder := []*sqlcv1.Tenant{{ID: uuid.New()}, {ID: uuid.New()}}

	tenantRepo := &fakeReapTenantRepo{
		listedNext: [][]*sqlcv1.Tenant{fullBatch, remainder},
	}
	rc := newReapTestController(&fakeReapRepo{tenantRepo: tenantRepo}, time.Hour)

	rc.runReapSoftDeletedTenants(context.Background())()

	assert.Len(t, tenantRepo.reaped, reapBatchSize+len(remainder),
		"every tenant from the full batch and the remainder is reaped in one pass")
	assert.Equal(t, reapBatchSize, tenantRepo.maxCountSeen,
		"the batch size is passed to the enumeration query")
}

// TestRunReapSoftDeletedTenantsStopsOnListError verifies that an
// enumeration failure aborts the pass without reaping anything.
func TestRunReapSoftDeletedTenantsStopsOnListError(t *testing.T) {
	tenantRepo := &fakeReapTenantRepo{listErr: assert.AnError}
	rc := newReapTestController(&fakeReapRepo{tenantRepo: tenantRepo}, time.Hour)

	rc.runReapSoftDeletedTenants(context.Background())()

	assert.Empty(t, tenantRepo.reaped,
		"nothing is reaped when the enumeration fails")
}
