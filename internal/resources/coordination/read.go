package coordination

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h handlerCoordination) Read(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	coordinationResource, err := ResourceSchemaToCoordinationResource(d)
	if err != nil {
		return diag.FromErr(err)
	}
	if coordinationResource == nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "got nil resource, unreachable code",
			},
		}
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: coordinationResource.DatabaseEndpoint,
		Token:            h.token,
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
	var description *coordination.NodeConfig
	_, description, err = db.Coordination().DescribeNode(ctx, coordinationResource.Path)
	if err != nil {
		if ydb.IsOperationErrorSchemeError(err) {
			d.SetId("")
			return nil
		}
		return diag.Errorf("failed to describe coordination %q: %s", coordinationResource.Path, err)
	}
	return diag.FromErr(flattenCoordinationDescription(d, description, coordinationResource.Entity))
}
