package topic

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func NewTopicResource(tokenCallback func(ctx context.Context) (string, error)) *schema.Provider {
	provider := &schema.Provider{

		DataSourcesMap: nil,
		ResourcesMap: map[string]*schema.Resource{
			"ydb_topic": resourceYcpYDBTopic(false),
		},
	}

	return provider
}

func Topic() {}
