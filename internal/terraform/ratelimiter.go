package terraform

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/ratelimiter"
)

func ydbRateLimiterResource() *schema.Resource {
	return &schema.Resource{
		Schema:        ratelimiter.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBRateLimiterCreate,
		ReadContext:   resourceYDBRateLimiterRead,
		UpdateContext: resourceYDBRateLimiterUpdate,
		DeleteContext: resourceYDBRateLimiterDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func ydbRateLimiterDataSource() *schema.Resource {
	return &schema.Resource{
		Schema:        ratelimiter.ResourceSchema(),
		SchemaVersion: 0,
		ReadContext:   dataSourceYDBRateLimiterRead,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func resourceYDBRateLimiterCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return ratelimiter.ResourceCreateFunc(cb)(ctx, d, meta)
}

func resourceYDBRateLimiterRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return ratelimiter.ResourceReadFunc(cb)(ctx, d, meta)
}

func resourceYDBRateLimiterUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return ratelimiter.ResourceUpdateFunc(cb)(ctx, d, meta)
}

func resourceYDBRateLimiterDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return ratelimiter.ResourceDeleteFunc(cb)(ctx, d, meta)
}

func dataSourceYDBRateLimiterRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return ratelimiter.ResourceReadFunc(cb)(ctx, d, meta)
}
