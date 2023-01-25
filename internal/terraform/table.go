package terraform

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/table"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/topic"
)

func ydbTableResource() *schema.Resource {
	return &schema.Resource{
		Schema:        table.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBTableCreate,
		ReadContext:   resourceYDBTableRead,
		UpdateContext: resourceYDBTableUpdate,
		DeleteContext: resourceYDBTableDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func ydbTableDataSource() *schema.Resource {
	return &schema.Resource{
		Schema:        table.ResourceSchema(),
		SchemaVersion: 0,
		ReadContext:   dataSourceYDBTableRead,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func resourceYDBTableCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return topic.ResourceCreateFunc(cb)(ctx, d, meta)
}

func resourceYDBTableRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return topic.ResourceReadFunc(cb)(ctx, d, meta)
}

func resourceYDBTableUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return topic.ResourceUpdateFunc(cb)(ctx, d, meta)
}

func resourceYDBTableDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return topic.ResourceDeleteFunc(cb)(ctx, d, meta)
}

func dataSourceYDBTableRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return topic.ResourceReadFunc(cb)(ctx, d, meta)
}
