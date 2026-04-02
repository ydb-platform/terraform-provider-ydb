package secret

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *handler) Create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	connectionString := d.Get("connection_string").(string)
	name := d.Get("name").(string)
	value := d.Get("value").(string)
	inheritPermissions := d.Get("inherit_permissions").(bool)

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

	q := fmt.Sprintf("CREATE SECRET `%s` WITH (value = '%s')", name, value)
	if inheritPermissions {
		q = fmt.Sprintf("CREATE SECRET `%s` WITH (value = '%s', inherit_permissions = True)", name, value)
	}
	err = db.Query().Exec(ctx, q)
	if err != nil {
		return diag.Diagnostics{
			{Severity: diag.Error, Summary: "failed to executing `" + q + "`", Detail: err.Error()},
		}
	}

	d.SetId(connectionString + "?path=" + name)

	return h.Read(ctx, d, meta)
}
