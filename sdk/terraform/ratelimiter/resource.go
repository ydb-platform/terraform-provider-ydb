package ratelimiter

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/internal/resources/ratelimiter"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func ResourceCreateFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		token, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}

		h := ratelimiter.NewHandler(token)
		return h.Create(ctx, d, meta)
	}
}

func ResourceUpdateFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		token, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}

		h := ratelimiter.NewHandler(token)
		return h.Update(ctx, d, meta)
	}
}

func ResourceDeleteFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		token, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}

		h := ratelimiter.NewHandler(token)
		return h.Delete(ctx, d, meta)
	}
}

func ResourceReadFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		token, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}

		h := ratelimiter.NewHandler(token)
		return h.Read(ctx, d, meta)
	}
}

func ResourceSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		"path": {
			Type:     schema.TypeString,
			Required: true,
			ForceNew: true,
		},
		"connection_string": {
			Type:     schema.TypeString,
			ForceNew: true,
			Required: true,
		},
		"resource_path": {
			Type:     schema.TypeString,
			ForceNew: true,
			Required: true,
		},
		"max_units_per_second": {
			Type:     schema.TypeFloat,
			Optional: true,
			Computed: true,
		},
		"max_burst_size_coefficient": {
			Type:     schema.TypeFloat,
			Optional: true,
			Computed: true,
		},
		"prefetch_coefficient": {
			Type:     schema.TypeFloat,
			Optional: true,
			Computed: true,
		},
		"prefetch_watermark": {
			Type:     schema.TypeFloat,
			Optional: true,
			Computed: true,
		},
	}
}
