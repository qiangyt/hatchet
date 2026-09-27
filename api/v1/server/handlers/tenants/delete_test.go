//go:build !e2e && !load && !rampup && !integration

package tenants

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/api/v1/server/oas/gen"
	"github.com/hatchet-dev/hatchet/pkg/config/database"
	"github.com/hatchet-dev/hatchet/pkg/config/server"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

type fakeDeleteRepo struct {
	v1.Repository
	tenant *fakeDeleteTenantRepo
}

func (f *fakeDeleteRepo) Tenant() v1.TenantRepository { return f.tenant }

type fakeDeleteTenantRepo struct {
	v1.TenantRepository

	member     *sqlcv1.PopulateTenantMembersRow
	memberErr  error
	deletedIds []uuid.UUID
	deleteErr  error
}

func (f *fakeDeleteTenantRepo) GetTenantMemberByUserID(ctx context.Context, tenantId, userId uuid.UUID) (*sqlcv1.PopulateTenantMembersRow, error) {
	return f.member, f.memberErr
}

func (f *fakeDeleteTenantRepo) DeleteTenant(ctx context.Context, id uuid.UUID) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deletedIds = append(f.deletedIds, id)
	return nil
}

func runTenantDelete(t *testing.T, tenantRepo *fakeDeleteTenantRepo) int {
	t.Helper()

	svc := NewTenantService(&server.ServerConfig{
		Layer: &database.Layer{V1: &fakeDeleteRepo{tenant: tenantRepo}},
	})

	tenant := &sqlcv1.Tenant{ID: uuid.New()}
	user := &sqlcv1.User{ID: uuid.New()}

	e := echo.New()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tenants/"+tenant.ID.String(), nil)
	rec := httptest.NewRecorder()
	echoCtx := e.NewContext(req, rec)
	echoCtx.Set("tenant", tenant)
	echoCtx.Set("user", user)

	resp, err := svc.TenantDelete(echoCtx, gen.TenantDeleteRequestObject{Tenant: tenant.ID})
	require.NoError(t, err)

	switch resp.(type) {
	case gen.TenantDelete204Response:
		return http.StatusNoContent
	case gen.TenantDelete403JSONResponse:
		return http.StatusForbidden
	}
	t.Fatalf("unexpected response type %T", resp)
	return 0
}

func TestTenantDeleteByOwnerSucceeds(t *testing.T) {
	tenantRepo := &fakeDeleteTenantRepo{
		member: &sqlcv1.PopulateTenantMembersRow{Role: sqlcv1.TenantMemberRoleOWNER},
	}

	code := runTenantDelete(t, tenantRepo)

	assert.Equal(t, http.StatusNoContent, code)
	assert.Len(t, tenantRepo.deletedIds, 1, "DeleteTenant is invoked for the tenant")
}

func TestTenantDeleteByNonOwnerIsForbidden(t *testing.T) {
	tenantRepo := &fakeDeleteTenantRepo{
		member: &sqlcv1.PopulateTenantMembersRow{Role: sqlcv1.TenantMemberRoleMEMBER},
	}

	code := runTenantDelete(t, tenantRepo)

	assert.Equal(t, http.StatusForbidden, code)
	assert.Empty(t, tenantRepo.deletedIds, "DeleteTenant must not run for non-owners")
}

func TestTenantDeleteWithoutMembershipIsForbidden(t *testing.T) {
	tenantRepo := &fakeDeleteTenantRepo{memberErr: pgx.ErrNoRows}

	code := runTenantDelete(t, tenantRepo)

	assert.Equal(t, http.StatusForbidden, code)
	assert.Empty(t, tenantRepo.deletedIds)
}

func TestTenantDeletePropagatesRepositoryError(t *testing.T) {
	tenantRepo := &fakeDeleteTenantRepo{
		member:    &sqlcv1.PopulateTenantMembersRow{Role: sqlcv1.TenantMemberRoleOWNER},
		deleteErr: errors.New("boom"),
	}

	svc := NewTenantService(&server.ServerConfig{
		Layer: &database.Layer{V1: &fakeDeleteRepo{tenant: tenantRepo}},
	})

	tenant := &sqlcv1.Tenant{ID: uuid.New()}
	user := &sqlcv1.User{ID: uuid.New()}

	e := echo.New()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tenants/"+tenant.ID.String(), nil)
	rec := httptest.NewRecorder()
	echoCtx := e.NewContext(req, rec)
	echoCtx.Set("tenant", tenant)
	echoCtx.Set("user", user)

	_, err := svc.TenantDelete(echoCtx, gen.TenantDeleteRequestObject{Tenant: tenant.ID})
	require.Error(t, err, "repository errors propagate to the global error handler")
	assert.Empty(t, tenantRepo.deletedIds)
}
