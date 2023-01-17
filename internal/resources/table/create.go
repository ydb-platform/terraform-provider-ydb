package table

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	tbl "github.com/ydb/terraform-provider-ydb/internal/table"
)

func strColumnUnitToYDBColumnUnit(cu string) options.TimeToLiveUnit {
	if cu == "ns" {
		return options.TimeToLiveUnitNanoseconds
	}
	if cu == "ms" {
		return options.TimeToLiveUnitMilliseconds
	}
	if cu == "us" {
		return options.TimeToLiveUnitMicroseconds
	}
	if cu == "s" {
		return options.TimeToLiveUnitSeconds
	}

	return options.TimeToLiveUnitUnspecified
}

func TableCreate(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
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
			},
		}
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	tableSession, err := db.Table().CreateSession(ctx)
	if err != nil {
		return diag.Diagnostics{
			{
				Severity: diag.Error,
				Summary:  "failed to create table session",
				Detail:   err.Error(),
			},
		}
	}
	defer func() {
		_ = tableSession.Close(ctx)
	}()

	err = nil
	//	err = tableSession.CreateTable(ctx, path, opts...)
	tableSession.Execute(ctx, nil, "", nil, nil)
	if err != nil {
		return diag.Diagnostics{
			{
				Severity: diag.Error,
				Summary:  "failed to create table",
				Detail:   err.Error(),
			},
		}
	}

	d.SetId(tableResource.Path)

	return TableRead(ctx, d, cfg)
}
