package terraform

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/table/index"
)

func ydbTableIndexResource() *schema.Resource {
	return &schema.Resource{
		Schema:        index.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBTableIndexCreate,
		ReadContext:   resourceYDBTableIndexRead,
		UpdateContext: resourceYDBTableIndexUpdate,
		DeleteContext: resourceYDBTableIndexDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func resourceYDBTableIndexCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return index.ResourceCreateFunc(cb)(ctx, d, meta)
}

func resourceYDBTableIndexRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return index.ResourceReadFunc(cb)(ctx, d, meta)
}

func resourceYDBTableIndexUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return index.ResourceUpdateFunc(cb)(ctx, d, meta)
}

func resourceYDBTableIndexDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return index.ResourceDeleteFunc(cb)(ctx, d, meta)
}
