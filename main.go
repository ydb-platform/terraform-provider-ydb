package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"

	"github.com/ydb-platform/terraform-provider-ydb/ydb"
)

func main() {
	opts := &plugin.ServeOpts{
		ProviderFunc: func() *schema.Provider {
			return ydb.Provider(nil)
		},
	}

	plugin.Serve(opts)
}
