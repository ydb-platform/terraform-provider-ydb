package terraform

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/coordination"
)

func ydbCoordinationResource() *schema.Resource {
	return &schema.Resource{
		Schema:        coordination.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBCoordinationCreate,
		ReadContext:   resourceYDBCoordinationRead,
		UpdateContext: resourceYDBCoordinationUpdate,
		DeleteContext: resourceYDBCoordinationDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func ydbCoordinationDataSource() *schema.Resource {
	return &schema.Resource{
		Schema:        coordination.ResourceSchema(),
		SchemaVersion: 0,
		ReadContext:   dataSourceYDBCoordinationRead,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func resourceYDBCoordinationCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return coordination.ResourceCreateFunc(cb)(ctx, d, meta)
}

func resourceYDBCoordinationRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return coordination.ResourceReadFunc(cb)(ctx, d, meta)
}

func resourceYDBCoordinationUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return coordination.ResourceUpdateFunc(cb)(ctx, d, meta)
}

func resourceYDBCoordinationDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return coordination.ResourceDeleteFunc(cb)(ctx, d, meta)
}

func dataSourceYDBCoordinationRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return coordination.ResourceReadFunc(cb)(ctx, d, meta)
}
