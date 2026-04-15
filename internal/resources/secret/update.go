package secret

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *handler) Update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	value, err := resolveSecretValue(ctx, d)
	if err != nil {
		return diag.FromErr(err)
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: entity.PrepareFullYDBEndpoint(),
		AuthCreds:        h.authCreds,
	})
	if err != nil {
		return diag.Diagnostics{
			{Severity: diag.Error, Summary: "failed to initialize table client", Detail: err.Error()},
		}
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	q := fmt.Sprintf("ALTER SECRET `%s` WITH (value = '%s')", helpers.EscapeYQLIdentifier(entity.GetEntityPath()), helpers.EscapeYQLString(value))
	err = db.Query().Exec(ctx, q)
	if err != nil {
		return diag.Diagnostics{
			{Severity: diag.Error, Summary: "failed to execute query ALTER SECRET ...", Detail: err.Error()},
		}
	}

	return h.Read(ctx, d, meta)
}
