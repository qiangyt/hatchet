package tenants

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"

	"github.com/hatchet-dev/hatchet/api/v1/server/oas/gen"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// TenantDelete soft-deletes the tenant. Only tenant owners may delete the
// tenant. Deleting an already-deleted tenant returns 404 via the tenant
// populator middleware, which filters soft-deleted tenants out.
func (t *TenantService) TenantDelete(ctx echo.Context, request gen.TenantDeleteRequestObject) (gen.TenantDeleteResponseObject, error) {
	tenant := ctx.Get("tenant").(*sqlcv1.Tenant)
	user := ctx.Get("user").(*sqlcv1.User)

	member, err := t.config.V1.Tenant().GetTenantMemberByUserID(ctx.Request().Context(), tenant.ID, user.ID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tenantDeleteForbidden(), nil
		}

		return nil, err
	}

	if member.Role != sqlcv1.TenantMemberRoleOWNER {
		return tenantDeleteForbidden(), nil
	}

	if err := t.config.V1.Tenant().DeleteTenant(ctx.Request().Context(), tenant.ID); err != nil {
		return nil, err
	}

	return gen.TenantDelete204Response{}, nil
}

func tenantDeleteForbidden() gen.TenantDelete403JSONResponse {
	code := uint64(403)

	return gen.TenantDelete403JSONResponse(
		gen.APIError{
			Code:        &code,
			Description: "Only tenant owners can delete the tenant",
		},
	)
}
