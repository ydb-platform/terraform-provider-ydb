package table

import (
	"context"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *handler) Read(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
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

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: tableResource.DatabaseEndpoint,
		Token:            tableResource.Token,
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
		_ = db.Close(ctx)
	}()

	var description options.Description
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		description, err = s.DescribeTable(ctx, tableResource.Path, options.WithPartitionStats(), options.WithShardKeyBounds(), options.WithTableStats())
		return err
	})
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
