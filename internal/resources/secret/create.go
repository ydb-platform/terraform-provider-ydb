package secret

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *handler) Create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	connectionString := d.Get("connection_string").(string)
	name := d.Get("name").(string)
	inheritPermissions := d.Get("inherit_permissions").(bool)

	value, err := resolveSecretValue(ctx, d)
	if err != nil {
		return diag.FromErr(err)
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: connectionString,
		AuthCreds:        h.authCreds,
	})
	if err != nil {
		return diag.Diagnostics{
			{Severity: diag.Error, Summary: "failed to initialize DB connection", Detail: err.Error()},
		}
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	err = db.Query().Exec(ctx, PrepareCreateQuery(name, value, inheritPermissions))
	if err != nil {
		return diag.Diagnostics{
			{Severity: diag.Error, Summary: "failed to executing `CREATE SECRET ...`", Detail: err.Error()},
		}
	}

	d.SetId(connectionString + "?path=" + name)

	return h.Read(ctx, d, meta)
}
