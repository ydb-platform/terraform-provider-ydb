package ratelimiter

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func rateLimiterDiff(d *schema.ResourceData) ratelimiter.Resource {
	var diff ratelimiter.Resource
	if d.HasChange("max_units_per_second") {
		v, _ := d.GetOk("max_units_per_second")
		diff.HierarchicalDrr.MaxBurstSizeCoefficient = v.(float64)
	} else {
		diff.HierarchicalDrr.MaxBurstSizeCoefficient = d.Get("max_units_per_second").(float64)
	}
	if d.HasChange("max_burst_size_coefficient") {
		v, _ := d.GetOk("max_burst_size_coefficient")
		diff.HierarchicalDrr.MaxBurstSizeCoefficient = v.(float64)
	} else {
		diff.HierarchicalDrr.MaxBurstSizeCoefficient = d.Get("max_burst_size_coefficient").(float64)
	}
	if d.HasChange("prefetch_coefficient") {
		v, _ := d.GetOk("prefetch_coefficient")
		diff.HierarchicalDrr.PrefetchCoefficient = v.(float64)
	} else {
		diff.HierarchicalDrr.PrefetchCoefficient = d.Get("prefetch_coefficient").(float64)
	}
	if d.HasChange("prefetch_watermark") {
		v, _ := d.GetOk("prefetch_watermark")
		diff.HierarchicalDrr.PrefetchWatermark = v.(float64)
	} else {
		diff.HierarchicalDrr.PrefetchWatermark = d.Get("prefetch_watermark").(float64)
	}
	if d.HasChange("resource_path") {
		v, _ := d.GetOk("resource_path")
		diff.ResourcePath = v.(string)
	} else {
		diff.ResourcePath = d.Get("resource_path").(string)
	}
	return diff
}

func (h handlerRateLimiter) Update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
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
	config := rateLimiterDiff(d)
	err = db.Ratelimiter().AlterResource(ctx, rateLimiterResource.Path, config)
	if err != nil {
		return diag.FromErr(err)
	}
	return h.Read(ctx, d, meta)
}
