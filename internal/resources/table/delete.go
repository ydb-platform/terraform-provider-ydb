package table

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	tbl "github.com/ydb/terraform-provider-ydb/internal/table"
)

func TableDelete(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
	tableResource := tableResourceSchemaToTableResource(d)
	if tableResource == nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "got nil resource, unreachable code",
			},
		}
	}

	client, err := tbl.CreateTableClient(ctx, tbl.TableClientParams{
		DatabaseEndpoint: tableResource.DatabaseEndpoint,
		Token:            "",
	})
	if err != nil {
		return diag.Errorf("failed to initialize table client: %s", err)
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	tableSession, err := client.CreateSession(ctx)
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
