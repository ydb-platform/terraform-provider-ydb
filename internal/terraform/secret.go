package terraform

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/secret"
)

func ydbSecretResource() *schema.Resource {
	return &schema.Resource{
		Schema:        secret.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBSecretCreate,
		ReadContext:   resourceYDBSecretRead,
		UpdateContext: resourceYDBSecretUpdate,
		DeleteContext: resourceYDBSecretDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func ydbSecretDataSource() *schema.Resource {
	return &schema.Resource{
		Schema:      secret.DataSourceSchema(),
		ReadContext: dataSourceYDBSecretRead,
		Timeouts:    defaultTimeouts(),
	}
}

func resourceYDBSecretCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return secret.ResourceCreateFunc(cb)(ctx, d, meta)
}

func resourceYDBSecretRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return secret.ResourceReadFunc(cb)(ctx, d, meta)
}

func resourceYDBSecretUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return secret.ResourceUpdateFunc(cb)(ctx, d, meta)
}

func resourceYDBSecretDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return secret.ResourceDeleteFunc(cb)(ctx, d, meta)
}

func dataSourceYDBSecretRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return secret.DataSourceReadFunc(cb)(ctx, d, meta)
}
