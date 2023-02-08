package terraform

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/changefeed"
)

func ydbTableChangefeedDataSource() *schema.Resource {
	return &schema.Resource{
		Schema:        changefeed.ResourceSchema(),
		SchemaVersion: 0,
		ReadContext:   dataSourceYDBTableChangefeedRead,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func resourceYDBTableChangefeedCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return changefeed.ResourceCreateFunc(cb)(ctx, d, meta)
}

func resourceYDBTableChangefeedRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return changefeed.ResourceReadFunc(cb)(ctx, d, meta)
}

func resourceYDBTableChangefeedUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return changefeed.ResourceUpdateFunc(cb)(ctx, d, meta)
}

func resourceYDBTableChangefeedDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return changefeed.ResourceDeleteFunc(cb)(ctx, d, meta)
}

func dataSourceYDBTableChangefeedRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return changefeed.ResourceReadFunc(cb)(ctx, d, meta)
}

func ydbChangeFeedResource() *schema.Resource {
	return &schema.Resource{
		Schema:        changefeed.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBTableChangefeedCreate,
		ReadContext:   resourceYDBTableChangefeedRead,
		UpdateContext: resourceYDBTableChangefeedUpdate,
		DeleteContext: resourceYDBTableChangefeedDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}