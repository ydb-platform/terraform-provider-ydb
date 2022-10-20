package ydb

import "github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

func Provider() *schema.Provider {
	provider := &schema.Provider{
		//		DataSourcesMap: map[string]*schema.Resource{
		//			"ydb_table": resourceYdbTable(),
		//		},
		Schema: map[string]*schema.Schema{
			"token": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("YDB_TOKEN", nil),
				Description: "Token for YDB access",
			},
		},

		ResourcesMap: map[string]*schema.Resource{
			"ydb_table": resourceYdbTable(),
		},
	}

	return provider
}
