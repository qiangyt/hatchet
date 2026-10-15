package olap

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"

	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// fakeTenantLookup stubs the internal-tenant lookup the partition cycle gates on.
type fakeTenantLookup struct {
	tenant *sqlcv1.Tenant
	err    error
}

func (f *fakeTenantLookup) GetInternalTenantForController(ctx context.Context) (*sqlcv1.Tenant, error) {
	return f.tenant, f.err
}

// fakeOLAPRepository records UpdateTablePartitions calls; every other method of
// the embedded interface stays nil because the partition cycle never calls them.
type fakeOLAPRepository struct {
	v1.OLAPRepository

	updatePartitionsErr error
	updateCalls         int
}

func (f *fakeOLAPRepository) UpdateTablePartitions(ctx context.Context) error {
	f.updateCalls++
	return f.updatePartitionsErr
}

func newTestLogger(buf *bytes.Buffer) *zerolog.Logger {
	l := zerolog.New(buf)
	return &l
}

// The incident this regression-pins: the remote engine lost its internal tenant
// row (soft-deleted then reaped); GetInternalTenantForController maps the
// missing row to (nil, nil) and the partition job returned silently, so no new
// daily partitions were created and every REST read against the OLAP tables
// died fleet-wide once the boot-time partitions ran out (docs/41).
func TestRunPartitionOnce_MissingInternalTenant_StillUpdatesPartitions(t *testing.T) {
	var buf bytes.Buffer
	repo := &fakeOLAPRepository{}
	lookup := &fakeTenantLookup{tenant: nil} // (nil, nil) = row missing

	runPartitionOnce(context.Background(), newTestLogger(&buf), lookup, repo)

	assert.Equal(t, 1, repo.updateCalls, "partition DDL must still run when the internal tenant is missing")
}

func TestRunPartitionOnce_MissingInternalTenant_LogsError(t *testing.T) {
	var buf bytes.Buffer
	repo := &fakeOLAPRepository{}
	lookup := &fakeTenantLookup{tenant: nil}

	runPartitionOnce(context.Background(), newTestLogger(&buf), lookup, repo)

	assert.Contains(t, buf.String(), `"level":"error"`, "missing internal tenant must be surfaced at error level, not silently skipped")
	assert.Contains(t, buf.String(), "internal tenant", "log must name the missing internal tenant")
}

func TestRunPartitionOnce_TenantPresent_UpdatesPartitions(t *testing.T) {
	var buf bytes.Buffer
	repo := &fakeOLAPRepository{}
	lookup := &fakeTenantLookup{tenant: &sqlcv1.Tenant{ID: uuid.New()}}

	runPartitionOnce(context.Background(), newTestLogger(&buf), lookup, repo)

	assert.Equal(t, 1, repo.updateCalls)
	assert.NotContains(t, buf.String(), `"level":"error"`)
}

func TestRunPartitionOnce_LookupError_SkipsDDLAndLogsError(t *testing.T) {
	var buf bytes.Buffer
	repo := &fakeOLAPRepository{}
	lookup := &fakeTenantLookup{err: errors.New("db down")}

	runPartitionOnce(context.Background(), newTestLogger(&buf), lookup, repo)

	assert.Equal(t, 0, repo.updateCalls, "a lookup error means we cannot trust the DB connection for DDL either")
	assert.Contains(t, buf.String(), `"level":"error"`)
}

func TestRunPartitionOnce_PartitionLockConflict_WarnsAndKeepsJobAlive(t *testing.T) {
	var buf bytes.Buffer
	repo := &fakeOLAPRepository{updatePartitionsErr: v1.ErrPartitionLockConflict}
	lookup := &fakeTenantLookup{tenant: &sqlcv1.Tenant{ID: uuid.New()}}

	runPartitionOnce(context.Background(), newTestLogger(&buf), lookup, repo)

	assert.Equal(t, 1, repo.updateCalls)
	assert.Contains(t, buf.String(), `"level":"warn"`, "lock conflict is an expected transient, warned not errored")
	assert.False(t, strings.Contains(buf.String(), `"level":"error"`), "lock conflict must not surface as an error")
}
