package terraform

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/topic/consumer"
)

func ydbTopicConsumer() *schema.Resource {
	return &schema.Resource{
		Schema:        consumer.ResourceConsumerSchema(),
		SchemaVersion: 0,
		CreateContext: resourceYDBTopicConsumerCreate,
		ReadContext:   resourceYDBTopicConsumerRead,
		UpdateContext: resourceYDBTopicConsumerUpdate,
		DeleteContext: resourceYDBTopicConsumerDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: defaultTimeouts(),
	}
}

func resourceYDBTopicConsumerCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return consumer.ResourceCreateConsumerFunc(cb)(ctx, d, meta)
}

func resourceYDBTopicConsumerRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return consumer.ResourceReadConsumerFunc(cb)(ctx, d, meta)
}

func resourceYDBTopicConsumerUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return consumer.ResourceUpdateConsumerFunc(cb)(ctx, d, meta)
}

func resourceYDBTopicConsumerDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cfg := meta.(*Config)
	cb := func(ctx context.Context) (string, error) {
		return cfg.Token, nil
	}

	return consumer.ResourceDeleteConsumerFunc(cb)(ctx, d, meta)
}
