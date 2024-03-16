package terraform

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/kv"
)

func ydbKvResource() *schema.Resource {
	return &schema.Resource{
		Schema:        kv.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBKvCreate,
		ReadContext:   resourceYDBKvRead,
		UpdateContext: resourceYDBKvUpdate,
		DeleteContext: resourceYDBKvDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func resourceYDBKvCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return kv.ResourceCreateFunc(cb)(ctx, d, meta)
}

func resourceYDBKvRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return kv.ResourceReadFunc(cb)(ctx, d, meta)
}

func resourceYDBKvUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return kv.ResourceUpdateFunc(cb)(ctx, d, meta)
}

func resourceYDBKvDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return kv.ResourceDeleteFunc(cb)(ctx, d, meta)
}
