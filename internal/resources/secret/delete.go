package secret

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *handler) Delete(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: entity.PrepareFullYDBEndpoint(),
		AuthCreds:        h.authCreds,
	})
	if err != nil {
		return diag.Errorf("failed to initialize table client: %s", err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	err = DropSecret(ctx, db, entity.GetEntityPath())
	if err != nil {
		return diag.Diagnostics{
			{Severity: diag.Error, Summary: "failed to execute query DROP SECRET ...", Detail: err.Error()},
		}
	}

	return nil
}
