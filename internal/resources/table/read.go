package table

import (
	"context"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	tbl "github.com/ydb/terraform-provider-ydb/internal/table"
)

func TableRead(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
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
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "failed to initialize table client",
				Detail:   err.Error(),
			}}
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	tableSession, err := db.Table().CreateSession(ctx)
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

	description, err := tableSession.DescribeTable(ctx, tableResource.Path, options.WithPartitionStats(), options.WithShardKeyBounds(), options.WithTableStats())
	if err != nil {
		if strings.Contains(err.Error(), "SCHEME_ERROR") {
			// NOTE(shmel1k@): marking as non-existing resource
			d.SetId("")
			return nil
		}
		return diag.Errorf("failed to describe table %q: %s", tableResource.Path, err)
	}

	flattenTableDescription(d, description, db.Name())
	return nil
}