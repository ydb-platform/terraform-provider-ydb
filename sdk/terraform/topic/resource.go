package topic

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers/topic"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/attributes"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

const (
	ydbTopicCodecGZIP = "gzip"
	ydbTopicCodecRAW  = "raw"
	ydbTopicCodecZSTD = "zstd"
)

func ResourceCreateFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		token, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}
		c := &caller{
			token: token,
		}
		return c.resourceYDBTopicCreate(ctx, d, meta)
	}
}

func ResourceReadFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		token, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}
		c := &caller{
			token: token,
		}
		return c.resourceYDBTopicRead(ctx, d, meta)
	}
}

func ResourceUpdateFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		token, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}
		c := &caller{
			token: token,
		}
		return c.resourceYDBTopicUpdate(ctx, d, meta)
	}
}

func ResourceDeleteFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		token, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}
		c := &caller{
			token: token,
		}
		return c.resourceYDBTopicDelete(ctx, d, meta)
	}
}

func DataSourceSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		"database_endpoint": {
			Type:     schema.TypeString,
			Optional: true,
		},
		"stream_id": {
			Type:     schema.TypeString,
			Optional: true,
		},
		"name": {
			Type:     schema.TypeString,
			Optional: true,
		},
		"description": {
			Type:     schema.TypeString,
			Computed: true,
		},
		"partitions_count": {
			Type:     schema.TypeInt,
			Optional: true,
		},
		"supported_codecs": {
			Type:     schema.TypeList,
			Optional: true,
			Elem: &schema.Schema{
				Type:         schema.TypeString,
				ValidateFunc: validation.StringInSlice(topic.YDBTopicAllowedCodecs, false),
			},
		},
		"retention_period_ms": {
			Type:     schema.TypeInt,
			Optional: true,
			Default:  1000 * 60 * 60 * 24, // 1 day
		},
		"consumer": {
			Type:     schema.TypeSet,
			Optional: true,
			Elem: &schema.Resource{
				Schema: map[string]*schema.Schema{
					"name": {
						Type:         schema.TypeString,
						Required:     true,
						ValidateFunc: validation.NoZeroValues,
					},
					"supported_codecs": {
						Type:     schema.TypeList,
						Optional: true,
						Elem: &schema.Schema{
							Type:         schema.TypeString,
							ValidateFunc: validation.StringInSlice(topic.YDBTopicAllowedCodecs, false),
						},
					},
					"starting_message_timestamp_ms": {
						Type:     schema.TypeInt,
						Optional: true,
					},
					"service_type": {
						Type:     schema.TypeString,
						Optional: true,
					},
				},
			},
		},
	}
}

func ResourceSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		attributes.DatabaseEndpoint: {
			Type:     schema.TypeString,
			Required: true,
			ForceNew: true,
		},
		attributeName: {
			Type:     schema.TypeString,
			Required: true,
			ForceNew: true,
		},
		attributes.Description: {
			Type:     schema.TypeString,
			Optional: true,
		},
		attributePartitionsCount: {
			Type:     schema.TypeInt,
			Optional: true,
			Computed: true,
		},
		attributeSupportedCodecs: {
			Type:     schema.TypeList,
			Optional: true,
			Elem: &schema.Schema{
				Type:         schema.TypeString,
				ValidateFunc: validation.StringInSlice(topic.YDBTopicAllowedCodecs, false),
			},
			Computed: true,
		},
		attributeRetentionPeriodHours: {
			Type:     schema.TypeInt,
			Optional: true,
			Computed: true,
			ConflictsWith: []string{
				attributeRetentionStorageMB,
			},
		},
		attributeRetentionStorageMB: {
			Type:     schema.TypeInt,
			Optional: true,
			Computed: true,
			ConflictsWith: []string{
				attributeRetentionPeriodHours,
			},
		},
		attributePartitionWriteSpeedKBPS: {
			Type:     schema.TypeInt,
			Optional: true,
			Computed: true,
		},
		attributeMeteringMode: {
			Type:     schema.TypeString,
			Optional: true,
			Computed: true,
		},
		attributeConsumer: {
			Type:     schema.TypeSet,
			Optional: true,
			Computed: true,
			Elem: &schema.Resource{
				Schema: map[string]*schema.Schema{
					attributeName: {
						Type:         schema.TypeString,
						Required:     true,
						ValidateFunc: validation.NoZeroValues,
					},
					attributeSupportedCodecs: {
						Type:     schema.TypeList,
						Optional: true,
						Elem: &schema.Schema{
							Type:         schema.TypeString,
							ValidateFunc: validation.StringInSlice(topic.YDBTopicAllowedCodecs, false),
						},
						Computed: true,
					},
					attributeConsumerStartingMessageTimestampMS: {
						Type:     schema.TypeInt,
						Optional: true,
						Computed: true,
					},
				},
			},
		},
	}
}
