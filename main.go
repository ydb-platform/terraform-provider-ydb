package main

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func main() {
	opts := &plugin.ServeOpts{
		ProviderFunc: nil,
	}
	_ = opts

	var sess table.Session
	sess.CreateTable(context.Background(), "")

	plugin.Serve(nil)
}
