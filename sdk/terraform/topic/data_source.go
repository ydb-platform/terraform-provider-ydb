package topic

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

type TopicProvider struct {
	tokenCallback auth.GetTokenCallback
}

func (t *TopicProvider) DataSource(deprecationMessage string) *schema.Resource {
	r := &schema.Resource{
		ReadContext: t.dataSourceYDBTopicRead,

		SchemaVersion: 0,
		Schema: map[string]*schema.Schema{
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
					ValidateFunc: validation.StringInSlice(ydbTopicAllowedCodecs, false),
				},
			},
			"retention_period_ms": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  1000 * 60 * 60 * 24, // 1 day
			},
			"consumer": {
				Type:     schema.TypeList,
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
								ValidateFunc: validation.StringInSlice(ydbTopicAllowedCodecs, false),
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
		},
	}
	r.DeprecationMessage = deprecationMessage

	return r
}

func NewProvider(cb auth.GetTokenCallback) *TopicProvider {
	return &TopicProvider{
		tokenCallback: cb,
	}
}

func (t *TopicProvider) dataSourceYDBTopicRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	client, err := t.createYDBConnection(ctx, d, nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-stream control plane client: %s", err))
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	description, err := client.Topic().Describe(ctx, d.Get("name").(string))
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			// stream was deleted outside from terraform.
			d.SetId("")
			return nil
		}
		return diag.FromErr(fmt.Errorf("datasource: failed to describe stream: %s", err))
	}

	err = flattenYDBTopicDescription(d, description)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to flatten stream description: %s", err))
	}

	return nil
}
