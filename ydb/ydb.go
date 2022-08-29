package ydb

import "github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

func Provider() *schema.Provider {
	provider := &schema.Provider{
		//		DataSourcesMap: map[string]*schema.Resource{
		//			"ydb_table": resourceYdbTable(),
		//		},
		ResourcesMap: map[string]*schema.Resource{
			"ydb_table": resourceYdbTable(),
		},
	}

	return provider
}
