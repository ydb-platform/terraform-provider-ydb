package terraform_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	eds "github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/externaldatasource"
)

func TestIAMExplicitResourceID(t *testing.T) {
	f := newIAMWireDB(t)
	d := schema.TestResourceDataRaw(t, eds.ResourceSchema(), map[string]interface{}{
		"connection_string":  fmt.Sprintf("grpc://%s/?database=/owner", f.addr),
		"path":               "eds",
		"source_type":        "Ydb",
		"location":           "target:2135",
		"database_name":      "/target",
		"auth_method":        "IAM",
		"service_account_id": "runtime-sa",
		"resource_id":        "explicit-cloud",
	})
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: "caller"}, nil
	}
	diags := eds.ResourceCreateFunc(cb)(context.Background(), d, nil)
	require.False(t, diags.HasError(), "%v", diags)
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, "explicit-cloud", f.properties["RESOURCE_ID"])
	require.Equal(t, "explicit-cloud", d.Get("resource_id"))
}

func TestIAMTargetChangeResolvesResourceID(t *testing.T) {
	for _, key := range []string{"database_name", "database_id", "location"} {
		t.Run(key, func(t *testing.T) {
			f := newIAMWireDB(t)
			d := iamWireData(t, f)
			cb := func(context.Context) (auth.YdbCredentials, error) {
				return auth.YdbCredentials{Token: "caller"}, nil
			}
			require.False(t, eds.ResourceCreateFunc(cb)(context.Background(), d, nil).HasError())
			r := &schema.Resource{Schema: eds.ResourceSchema(), CustomizeDiff: eds.CustomizeDiff, UpdateContext: schema.UpdateContextFunc(eds.ResourceUpdateFunc(cb))}
			cfg := iamResourceConfig(f)
			cfg[key] = "/updated-target"
			state := d.State()
			diff, err := iamResourceDiff(r, state, cfg)
			require.NoError(t, err)
			require.NotNil(t, diff.Attributes["resource_id"], "target changes must invalidate the observed cloud ID")
			require.True(t, diff.Attributes["resource_id"].NewComputed)
			next, diags := r.Apply(context.Background(), state, diff, nil)
			require.False(t, diags.HasError(), "%v", diags)
			require.Equal(t, d.Id(), next.ID)
			require.Equal(t, "resolved:"+cfg["database_name"].(string), next.Attributes["resource_id"])
		})
	}
}

func TestIAMImplicitCloudIDRequiresDatabasePath(t *testing.T) {
	r := &schema.Resource{Schema: eds.ResourceSchema(), CustomizeDiff: eds.CustomizeDiff}
	cfg := iamResourceConfig(&iamWireDB{addr: "localhost:2136"})
	delete(cfg, "database_name")
	cfg["database_id"] = "target-id"
	_, err := iamResourceDiff(r, nil, cfg)
	require.ErrorContains(t, err, "DATABASE_NAME")
}

func TestIAMDatabaseTargetPlans(t *testing.T) {
	for _, tc := range []struct {
		name, databaseName, resourceID string
		wantError                      bool
	}{
		{name: "full path", databaseName: "/target"},
		{name: "blank path", databaseName: "  ", wantError: true},
		{name: "relative path", databaseName: "target", wantError: true},
		{name: "explicit cloud ID", resourceID: "cloud"},
		{name: "unknown path", databaseName: "74D93920-ED26-11E3-AC10-0800200C9A66"},
		{name: "unknown cloud ID", resourceID: "74D93920-ED26-11E3-AC10-0800200C9A66"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &schema.Resource{Schema: eds.ResourceSchema(), CustomizeDiff: eds.CustomizeDiff}
			cfg := iamResourceConfig(&iamWireDB{addr: "localhost:2136"})
			cfg["database_name"] = tc.databaseName
			cfg["database_id"] = "target-id"
			if tc.resourceID != "" {
				cfg["resource_id"] = tc.resourceID
			}
			_, err := iamResourceDiff(r, nil, cfg)
			if tc.wantError {
				require.ErrorContains(t, err, "DATABASE_NAME")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIAMUnknownAuthPlan(t *testing.T) {
	for _, key := range []string{"auth_method", "service_account_id"} {
		t.Run(key, func(t *testing.T) {
			r := &schema.Resource{Schema: eds.ResourceSchema(), CustomizeDiff: eds.CustomizeDiff}
			cfg := iamResourceConfig(&iamWireDB{addr: "localhost:2136"})
			cfg[key] = "74D93920-ED26-11E3-AC10-0800200C9A66"
			_, err := iamResourceDiff(r, nil, cfg)
			require.NoError(t, err)
		})
	}
}

func iamResourceConfig(f *iamWireDB) map[string]interface{} {
	return map[string]interface{}{
		"connection_string": fmt.Sprintf("grpc://%s/?database=/owner", f.addr), "path": "eds",
		"source_type": "Ydb", "location": "target:2135", "database_name": "/target",
		"auth_method": "IAM", "service_account_id": "runtime-sa",
	}
}

func iamResourceDiff(r *schema.Resource, state *terraform.InstanceState, cfg map[string]interface{}) (*terraform.InstanceDiff, error) {
	if state == nil {
		state = &terraform.InstanceState{}
	}
	raw := map[string]cty.Value{}
	for k, attr := range r.Schema {
		if attr.Type == schema.TypeBool {
			raw[k] = cty.NullVal(cty.Bool)
		} else {
			raw[k] = cty.NullVal(cty.String)
		}
	}
	for k, v := range cfg {
		if b, ok := v.(bool); ok {
			raw[k] = cty.BoolVal(b)
		} else if v == "74D93920-ED26-11E3-AC10-0800200C9A66" {
			raw[k] = cty.UnknownVal(cty.String)
		} else {
			raw[k] = cty.StringVal(v.(string))
		}
	}
	state.RawConfig = cty.ObjectVal(raw)
	return r.Diff(context.Background(), state, terraform.NewResourceConfigRaw(cfg), nil)
}
