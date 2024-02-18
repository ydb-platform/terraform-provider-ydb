package terraform

import (
	"context"
	"time"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type Config struct {
	Endpoint  string
	AuthCreds auth.YdbCredentials
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
			"user": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"password": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
		DataSourcesMap: map[string]*schema.Resource{
			"ydb_topic":        ydbTopicDataSource(),
			"ydb_table":        ydbTableDataSource(),
			"ydb_coordination": ydbCoordinationDataSource(),
			"ydb_rate_limiter": ydbRateLimiterDataSource(),
		},
		ResourcesMap: map[string]*schema.Resource{
			"ydb_topic":            ydbTopicResource(),
			"ydb_table":            ydbTableResource(),
			"ydb_table_changefeed": ydbTableChangeFeedResource(),
			"ydb_table_index":      ydbTableIndexResource(),
			"ydb_coordination":     ydbCoordinationResource(),
			"ydb_ratelimiter":      ydbRateLimiterResource(),
		},
	}

	provider.ConfigureContextFunc = configureProvider
	return provider
}

func configureProvider(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	cfg := &Config{
		Endpoint: d.Get("endpoint").(string),
		AuthCreds: auth.YdbCredentials{
			Token:    d.Get("token").(string),
			User:     d.Get("user").(string),
			Password: d.Get("password").(string),
		},
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
