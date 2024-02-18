package ratelimiter

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h handlerRateLimiter) Read(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
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
	var description *ratelimiter.Resource
	description, err = db.Ratelimiter().DescribeResource(ctx, rateLimiterResource.Path, rateLimiterResource.ResourcePath)
	if err != nil {
		if ydb.IsOperationErrorSchemeError(err) {
			d.SetId("")
			return nil
		}
		return diag.Errorf("failed to describe ratelimiter %q: %s", rateLimiterResource.Path, err)
	}
	return diag.FromErr(flattenRateLimiterDescription(d, description, rateLimiterResource.Entity))
}
