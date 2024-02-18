package terraform

import (
	"context"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/topic"
)

func ydbTopicResource() *schema.Resource {
	return &schema.Resource{
		Schema:        topic.ResourceSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBTopicCreate,
		ReadContext:   resourceYDBTopicRead,
		UpdateContext: resourceYDBTopicUpdate,
		DeleteContext: resourceYDBTopicDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func ydbTopicDataSource() *schema.Resource {
	return &schema.Resource{
		Schema:        topic.DataSourceSchema(),
		SchemaVersion: 0,
		ReadContext:   dataSourceYDBTopicRead,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func dataSourceYDBTopicRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return topic.DataSourceReadFunc(cb)(ctx, d, meta)
}

func resourceYDBTopicCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return topic.ResourceCreateFunc(cb)(ctx, d, meta)
}

func resourceYDBTopicRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return topic.ResourceReadFunc(cb)(ctx, d, meta)
}

func resourceYDBTopicUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return topic.ResourceUpdateFunc(cb)(ctx, d, meta)
}

func resourceYDBTopicDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (auth.YdbCredentials, error) {
		return cfg.AuthCreds, nil
	}

	return topic.ResourceDeleteFunc(cb)(ctx, d, meta)
}
