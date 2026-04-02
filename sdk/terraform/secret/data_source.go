package secret

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	secretHandler "github.com/ydb-platform/terraform-provider-ydb/internal/resources/secret"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func DataSourceReadFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{Severity: diag.Error, Summary: "failed to create token for YDB request", Detail: err.Error()},
			}
		}

		connectionString := d.Get("connection_string").(string)
		name := d.Get("name").(string)
		d.SetId(connectionString + "?path=" + name)

		h := secretHandler.NewHandler(authCreds)
		return h.Read(ctx, d, meta)
	}
}
