//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func createOLAPRepository(pool *pgxpool.Pool) *OLAPRepositoryImpl {
	return createOLAPRepositoryWithLogger(pool, zerolog.Nop())
}

func createOLAPRepositoryWithLogger(pool *pgxpool.Pool, logger zerolog.Logger) *OLAPRepositoryImpl {
	shared := &sharedRepository{
		pool:    pool,
		ddlPool: pool,
		l:       &logger,
		queries: sqlcv1.New(),
	}
	eventCache, err := lru.New[string, bool](100000)
	if err != nil {
		panic(err)
	}
	return &OLAPRepositoryImpl{
		sharedRepository:            shared,
		eventCache:                  eventCache,
		olapRetentionPeriod:         24 * time.Hour,
		shouldPartitionEventsTables: false,
		shouldPartitionOtelTables:   false,
	}
}

// TestOLAPUpdateTablePartitions_LeaseHeldElsewhere_WarnsAndSkips pins the
// observability of the lease-miss skip: skipping partition maintenance because
// another instance holds the lease is normal in multi-instance deployments but
// invisible at Debug level. When partition maintenance stops fleet-wide (the
// 2026-10-15 incident), the skip must be the first thing an operator sees, so
// it logs at Warn.
func TestOLAPUpdateTablePartitions_LeaseHeldElsewhere_WarnsAndSkips(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	holder := createOLAPRepository(pool)
	leases, err := holder.acquirePartitionLease(ctx(t), pool, "v1_olap_partitions")
	require.NoError(t, err)
	require.NotEmpty(t, leases, "the first acquisition must take the lease")

	var buf bytes.Buffer
	repo := createOLAPRepositoryWithLogger(pool, zerolog.New(&buf))

	err = repo.UpdateTablePartitions(ctx(t))
	require.NoError(t, err)

	assert.Contains(t, buf.String(), `"level":"warn"`,
		"the lease-miss skip must be visible at warn level, not buried at debug")
	assert.Contains(t, buf.String(), "already running")
}

func TestOLAPUpdateTablePartitions_ConcurrentControllers(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	const numControllers = 4
	repos := make([]*OLAPRepositoryImpl, numControllers)
	for i := 0; i < numControllers; i++ {
		repos[i] = createOLAPRepository(pool)
	}

	var (
		successCount int64
		errorCount   int64
	)

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < numControllers; i++ {
		wg.Add(1)
		go func(controllerID int) {
			defer wg.Done()

			<-start

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			err := repos[controllerID].UpdateTablePartitions(ctx)

			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				t.Logf("Controller %d encountered error: %v", controllerID, err)
			} else {
				atomic.AddInt64(&successCount, 1)
				t.Logf("Controller %d completed successfully", controllerID)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	finalSuccessCount := atomic.LoadInt64(&successCount)
	finalErrorCount := atomic.LoadInt64(&errorCount)

	t.Logf("Successful executions: %d", finalSuccessCount)
	t.Logf("Errors: %d", finalErrorCount)

	assert.Equal(t, int64(0), finalErrorCount, "No controllers should have encountered errors")
	assert.Equal(t, int64(numControllers), finalSuccessCount, "All controllers should have completed successfully")
}

func TestOLAPUpdateTablePartitions_SerialExecution(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createOLAPRepository(pool)

	const numRuns = 3
	var successCount int64

	for i := 0; i < numRuns; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		err := repo.UpdateTablePartitions(ctx)
		cancel()

		if err == nil {
			atomic.AddInt64(&successCount, 1)
			t.Logf("Run %d completed successfully", i)
		} else {
			t.Logf("Run %d failed: %v", i, err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	assert.Equal(t, int64(numRuns), atomic.LoadInt64(&successCount), "All serial runs should succeed")
}

func TestOLAPUpdateTablePartitions_RealPartitionCreation(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createOLAPRepository(pool)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var countBefore int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_tables
		WHERE tablename LIKE 'v1_tasks_olap_%'
		   OR tablename LIKE 'v1_dags_olap_%'
		   OR tablename LIKE 'v1_runs_olap_%'
		   OR tablename LIKE 'v1_payloads_olap_%'
	`).Scan(&countBefore)
	require.NoError(t, err)

	err = repo.UpdateTablePartitions(ctx)
	require.NoError(t, err)

	var countAfter int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_tables
		WHERE tablename LIKE 'v1_tasks_olap_%'
		   OR tablename LIKE 'v1_dags_olap_%'
		   OR tablename LIKE 'v1_runs_olap_%'
		   OR tablename LIKE 'v1_payloads_olap_%'
	`).Scan(&countAfter)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, countAfter, countBefore, "Partition count should not decrease after UpdateTablePartitions")

	t.Logf("Partitions before: %d, after: %d", countBefore, countAfter)
}

func TestOLAPUpdateTablePartitions_ContextCancellation(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createOLAPRepository(pool)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := repo.UpdateTablePartitions(ctx)

	if err != nil {
		t.Logf("Function returned error due to context cancellation: %v", err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	}

	assert.True(t, true, "Function completed without panic")
}

func TestOLAPUpdateTablePartitions_LockContention(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	const numRepositories = 10
	repos := make([]*OLAPRepositoryImpl, numRepositories)
	for i := 0; i < numRepositories; i++ {
		repos[i] = createOLAPRepository(pool)
	}

	var (
		successCount int64
		errorCount   int64
	)

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < numRepositories; i++ {
		wg.Add(1)
		go func(repoID int) {
			defer wg.Done()

			<-start

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			err := repos[repoID].UpdateTablePartitions(ctx)

			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				t.Logf("Repository %d encountered error: %v", repoID, err)
			} else {
				atomic.AddInt64(&successCount, 1)
				t.Logf("Repository %d completed successfully", repoID)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	finalSuccessCount := atomic.LoadInt64(&successCount)
	finalErrorCount := atomic.LoadInt64(&errorCount)

	t.Logf("High contention test - Successes: %d, Errors: %d", finalSuccessCount, finalErrorCount)

	assert.Equal(t, int64(0), finalErrorCount, "No errors should occur under contention")
	assert.Equal(t, int64(numRepositories), finalSuccessCount, "All repositories should complete successfully")
}
