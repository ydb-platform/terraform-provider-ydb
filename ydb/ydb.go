package ydb

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/topic"
)

func Provider(tokenCallback auth.GetTokenCallback) *schema.Provider {
	topicProvider := topic.NewProvider(nil)
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"endpoint": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"ydb_token": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},

		DataSourcesMap: map[string]*schema.Resource{
			"ydb_topic": topicProvider.DataSource(""),
		},

		ResourcesMap: map[string]*schema.Resource{
			"ydb_topic": topicProvider.Resource(""),
		},
	}
}
