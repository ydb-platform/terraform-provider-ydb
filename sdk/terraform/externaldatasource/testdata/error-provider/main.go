// Test-only fixture characterizing Terraform Core's handling of partial Create errors.
package main

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	eds "github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/externaldatasource"
	"os"
	"strconv"
)

func main() {
	if os.Getenv("YDB_TEST_REAL_PROVIDER") == "1" {
		if p := os.Getenv("YDB_TEST_PID_FILE"); p != "" {
			_ = os.WriteFile(p, []byte(strconv.Itoa(os.Getpid())), 0600)
		}
		cb := func(context.Context) (auth.YdbCredentials, error) {
			return auth.YdbCredentials{Token: "synthetic-caller-'\\-token"}, nil
		}
		plugin.Serve(&plugin.ServeOpts{ProviderFunc: func() *schema.Provider {
			return &schema.Provider{ResourcesMap: map[string]*schema.Resource{"test_eds": {Schema: eds.ResourceSchema(), CustomizeDiff: eds.CustomizeDiff, CreateContext: schema.CreateContextFunc(eds.ResourceCreateFunc(cb)), ReadContext: schema.ReadContextFunc(eds.ResourceReadFunc(cb)), UpdateContext: schema.UpdateContextFunc(eds.ResourceUpdateFunc(cb)), DeleteContext: schema.DeleteContextFunc(eds.ResourceDeleteFunc(cb))}}}
		}})
		return
	}
	plugin.Serve(&plugin.ServeOpts{ProviderFunc: func() *schema.Provider {
		return &schema.Provider{ResourcesMap: map[string]*schema.Resource{"test_eds": {
			Schema: map[string]*schema.Schema{"path": {Type: schema.TypeString, Required: true, ForceNew: true}, "pending_cleanup_path": {Type: schema.TypeString, Computed: true}},
			CreateContext: func(_ context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
				d.SetId("confirmed-eds")
				_ = d.Set("pending_cleanup_path", "/owner/confirmed-orphan")
				return diag.Errorf("EDS created; bootstrap cleanup failed")
			},
			ReadContext: func(context.Context, *schema.ResourceData, interface{}) diag.Diagnostics { return nil },
			DeleteContext: func(_ context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
				d.SetId("")
				return nil
			},
		}}}
	}})
}
