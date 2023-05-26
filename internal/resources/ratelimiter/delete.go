package ratelimiter

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h handlerRateLimiter) Delete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	rateLimiterResource, err := ResourceSchemaToRateLimiterResource(d)
	if err != nil {
		return diag.FromErr(err)
	}
	if rateLimiterResource == nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "got nil resource, unreachable code",
			},
		}
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: rateLimiterResource.DatabaseEndpoint,
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

	err = db.Ratelimiter().DropResource(ctx, rateLimiterResource.Path, rateLimiterResource.ResourcePath)
	if err != nil {
		return diag.Errorf("failed to drop ratelimiter %q: %s", rateLimiterResource.Path, err)
	}
	return nil
}
