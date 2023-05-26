package ratelimiter

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h handlerRateLimiter) Create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
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
	id := rateLimiterResource.DatabaseEndpoint + "?path=" + rateLimiterResource.Path
	d.SetId(id)
	err = db.Ratelimiter().CreateResource(ctx, rateLimiterResource.Path, ResourceToRateLimiterResource(rateLimiterResource))
	if err != nil {
		return diag.FromErr(err)
	}
	return h.Read(ctx, d, meta)
}
