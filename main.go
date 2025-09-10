package main

import (
	"flag"

	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"

	"github.com/ydb-platform/terraform-provider-ydb/ydb"
)

func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "set to true to run the provider with support for debuggers like delve")
	flag.Parse()

	opts := &plugin.ServeOpts{
		ProviderAddr: "terraform.storage.ydb.tech/provider/ydb",
		ProviderFunc: ydb.Provider,
		Debug:        debug,
	}

	plugin.Serve(opts)
}
