package externaldatasource

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

func (h *Handler) Delete(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	if diags := h.recoverCleanup(ctx, d, entity.PrepareFullYDBEndpoint()); diags.HasError() {
		return diags
	}
	db, err := h.open(ctx, entity.PrepareFullYDBEndpoint())
	if err != nil {
		return diag.Errorf("failed to initialize client: %s", safeStatus(err))
	}
	defer h.close(ctx, db)

	q := PrepareDropQuery(entity.GetFullEntityPath())
	err = db.Exec(ctx, q)
	if err != nil {
		return diag.Errorf("failed to drop external data source %q: %s", entity.GetEntityPath(), safeStatus(err))
	}

	return nil
}
