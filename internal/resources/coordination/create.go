package coordination

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h handlerCoordination) Create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
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
	id := coordinationResource.DatabaseEndpoint + "?path=" + coordinationResource.Path
	d.SetId(id)
	err = db.Coordination().CreateNode(ctx, coordinationResource.Path, ResourceToNodeConfig(coordinationResource))
	if err != nil {
		return nil
	}
	return h.Read(ctx, d, meta)
}
