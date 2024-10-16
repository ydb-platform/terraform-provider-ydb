package coordination

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h handlerCoordination) Delete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
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
		AuthCreds:        h.authCreds,
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

	err = db.Coordination().DropNode(ctx, coordinationResource.toFullPath())
	if err != nil {
		return diag.Errorf("failed to drop coordination %q: %s", coordinationResource.toFullPath(), err)
	}
	return nil
}
