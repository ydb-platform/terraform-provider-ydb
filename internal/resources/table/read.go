package table

import (
	"context"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	tbl "github.com/ydb/terraform-provider-ydb/internal/table"
)

func TableRead(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
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
		Token:            "", // TODO(shmel1k@): add token
	})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "failed to initialize table client",
				Detail:   err.Error(),
			},
		}
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	tableSession, err := client.CreateSession(ctx)
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "failed to create table-client session",
				Detail:   err.Error(),
			},
		}
	}
	defer func() {
		_ = tableSession.Close(ctx)
	}()

	description, err := tableSession.DescribeTable(ctx, tableResource.Path)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			// NOTE(shmel1k@): marking as non-existing resource
			d.SetId("")
			return nil
		}
		return nil
	}

	flattenTableDescription(d, description)
	return nil
}
