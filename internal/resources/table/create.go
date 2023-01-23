package table

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func Create(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
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
	tableResource.Token = "mytoken" // TODO(shmel1k@):
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

	// panic(q)
	sess, err := db.Table().CreateSession(ctx)
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "failed to initialize table session",
				Detail:   err.Error(),
			},
		}
	}
	q := PrepareCreateRequest(tableResource)
	err = sess.ExecuteSchemeQuery(ctx, q)
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

	return Read(ctx, d, cfg)
}
