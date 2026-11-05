package externaldatasource

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *Handler) Delete(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	if diags := guardIAMMutation(d); diags.HasError() {
		return diags
	}
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: entity.PrepareFullYDBEndpoint(),
		AuthCreds:        h.authCreds,
	})
	if err != nil {
		return diag.Errorf("failed to initialize client: %s", iamStatus(err))
	}
	defer func() { _ = db.Close(ctx) }()
	if diags := recoverIAMSecret(ctx, d, db); diags.HasError() {
		return diags
	}

	q := PrepareDropQuery(entity.GetFullEntityPath())
	err = db.Query().Exec(ctx, q)
	if err != nil {
		return diag.Errorf("failed to drop external data source %q: %s", entity.GetEntityPath(), iamStatus(err))
	}

	return nil
}
