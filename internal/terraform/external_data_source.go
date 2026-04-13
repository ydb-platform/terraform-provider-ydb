package terraform

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/resources/externaldatasource"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	eds "github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/externaldatasource"
)

func ydbExternalDataSourceResource() *schema.Resource {
	return &schema.Resource{
		Schema:        eds.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBExternalDataSourceCreate,
		ReadContext:   resourceYDBExternalDataSourceRead,
		DeleteContext: resourceYDBExternalDataSourceDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		CustomizeDiff: func(_ context.Context, d *schema.ResourceDiff, _ interface{}) error {
			if err := externaldatasource.ValidateResourceDiffAuth(d); err != nil {
				return err
			}
			return externaldatasource.ValidateResourceDiffSourceType(d)
		},
		Timeouts: defaultTimeouts(),
	}
}

func ydbExternalDataSourceDataSource() *schema.Resource {
	return &schema.Resource{
		Schema:      eds.DataSourceSchema(),
		ReadContext: dataSourceYDBExternalDataSourceRead,
		Timeouts:    defaultTimeouts(),
	}
}

func resourceYDBExternalDataSourceCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return eds.ResourceCreateFunc(cb)(ctx, d, meta)
}

func resourceYDBExternalDataSourceRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return eds.ResourceReadFunc(cb)(ctx, d, meta)
}

func resourceYDBExternalDataSourceDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return eds.ResourceDeleteFunc(cb)(ctx, d, meta)
}

func dataSourceYDBExternalDataSourceRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
	return eds.DataSourceReadFunc(cb)(ctx, d, meta)
}
