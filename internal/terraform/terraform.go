package terraform

import (
	"context"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type Config struct {
	Endpoint string
	Token    string
}

func Provider() *schema.Provider {
	provider := &schema.Provider{
		Schema: map[string]*schema.Schema{
			"endpoint": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"token": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
		DataSourcesMap: map[string]*schema.Resource{
			"ydb_topic": ydbTopicDataSource(),
			"ydb_table": ydbTableDataSource(),
		},
		ResourcesMap: map[string]*schema.Resource{
			"ydb_topic": ydbTopicResource(),
			"ydb_table": ydbTableResource(),
		},
	}

	provider.ConfigureContextFunc = configureProvider
	return provider
}

func configureProvider(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	cfg := &Config{
		Endpoint: d.Get("endpoint").(string),
		Token:    d.Get("token").(string),
	}
	return cfg, nil
}

func defaultTimeouts() *schema.ResourceTimeout {
	return &schema.ResourceTimeout{
		Create:  schema.DefaultTimeout(time.Minute * 20),
		Read:    schema.DefaultTimeout(time.Minute * 20),
		Update:  schema.DefaultTimeout(time.Minute * 20),
		Delete:  schema.DefaultTimeout(time.Minute * 20),
		Default: schema.DefaultTimeout(time.Minute * 20),
	}
}
