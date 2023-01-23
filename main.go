package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"

	"github.com/ydb-platform/terraform-provider-ydb/ydb"
)

func main() {
	opts := &plugin.ServeOpts{
		ProviderFunc: ydb.Provider,
	}

	plugin.Serve(opts)
}
