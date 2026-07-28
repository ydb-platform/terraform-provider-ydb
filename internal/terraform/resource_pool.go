package terraform

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/resourcepool"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/resourcepoolclassifier"
)

func ydbResourcePoolResource() *schema.Resource {
	return &schema.Resource{
		Schema:        resourcepool.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBResourcePoolCreate,
		ReadContext:   resourceYDBResourcePoolRead,
		UpdateContext: resourceYDBResourcePoolUpdate,
		DeleteContext: resourceYDBResourcePoolDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func ydbResourcePoolClassifierResource() *schema.Resource {
	return &schema.Resource{
		Schema:        resourcepoolclassifier.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBResourcePoolClassifierCreate,
		ReadContext:   resourceYDBResourcePoolClassifierRead,
		UpdateContext: resourceYDBResourcePoolClassifierUpdate,
		DeleteContext: resourceYDBResourcePoolClassifierDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func resourceYDBResourcePoolCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return resourcepool.ResourceCreateFunc(authCallback(meta))(ctx, d, meta)
}

func resourceYDBResourcePoolRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return resourcepool.ResourceReadFunc(authCallback(meta))(ctx, d, meta)
}

func resourceYDBResourcePoolUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return resourcepool.ResourceUpdateFunc(authCallback(meta))(ctx, d, meta)
}

func resourceYDBResourcePoolDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return resourcepool.ResourceDeleteFunc(authCallback(meta))(ctx, d, meta)
}

func resourceYDBResourcePoolClassifierCreate(
	ctx context.Context,
	d *schema.ResourceData,
	meta interface{},
) diag.Diagnostics {
	return resourcepoolclassifier.ResourceCreateFunc(authCallback(meta))(ctx, d, meta)
}

func resourceYDBResourcePoolClassifierRead(
	ctx context.Context,
	d *schema.ResourceData,
	meta interface{},
) diag.Diagnostics {
	return resourcepoolclassifier.ResourceReadFunc(authCallback(meta))(ctx, d, meta)
}

func resourceYDBResourcePoolClassifierUpdate(
	ctx context.Context,
	d *schema.ResourceData,
	meta interface{},
) diag.Diagnostics {
	return resourcepoolclassifier.ResourceUpdateFunc(authCallback(meta))(ctx, d, meta)
}

func resourceYDBResourcePoolClassifierDelete(
	ctx context.Context,
	d *schema.ResourceData,
	meta interface{},
) diag.Diagnostics {
	return resourcepoolclassifier.ResourceDeleteFunc(authCallback(meta))(ctx, d, meta)
}

func authCallback(meta interface{}) auth.GetAuthCallback {
	cfg := meta.(*Config)
	return func(context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}
}
