package table

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	tbl "github.com/ydb/terraform-provider-ydb/internal/table"
)

func TableDelete(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
	tableResource, err := tableResourceSchemaToTableResource(d)
	if err != nil {
		return diag.FromErr(err)
	}
	if tableResource == nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "got nil resource, unreachable code",
			},
		}
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.TableClientParams{
		DatabaseEndpoint: tableResource.DatabaseEndpoint,
		Token:            tableResource.Token,
	})
	if err != nil {
		return diag.Errorf("failed to initialize table client: %s", err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	tableSession, err := db.Table().CreateSession(ctx)
	if err != nil {
		return diag.Errorf("failed to create table session: %s", err)
	}
	defer func() {
		_ = tableSession.Close(ctx)
	}()

	err = tableSession.DropTable(ctx, tableResource.Path)
	if err != nil {
		return diag.Errorf("failed to drop table %q: %s", tableResource.Path, err)
	}
	return nil
}
