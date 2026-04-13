package terraform

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	et "github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/externaltable"
)

func ydbExternalTableResource() *schema.Resource {
	return &schema.Resource{
		Schema:        et.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBExternalTableCreate,
		ReadContext:   resourceYDBExternalTableRead,
		DeleteContext: resourceYDBExternalTableDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func ydbExternalTableDataSource() *schema.Resource {
	return &schema.Resource{
		Schema:      et.DataSourceSchema(),
		ReadContext: dataSourceYDBExternalTableRead,
		Timeouts:    defaultTimeouts(),
	}
}

func resourceYDBExternalTableCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return et.ResourceCreateFunc(cb)(ctx, d, meta)
}

func resourceYDBExternalTableRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return et.ResourceReadFunc(cb)(ctx, d, meta)
}

func resourceYDBExternalTableDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return et.ResourceDeleteFunc(cb)(ctx, d, meta)
}

func dataSourceYDBExternalTableRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return et.DataSourceReadFunc(cb)(ctx, d, meta)
}
