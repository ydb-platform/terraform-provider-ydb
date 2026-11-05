package terraform_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	eds "github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/externaldatasource"
	ydbprovider "github.com/ydb-platform/terraform-provider-ydb/ydb"
)

func TestIAMTargetChangeUsesServerResolution(t *testing.T) {
	f := newIAMWireDB(t)
	d := iamWireData(t, f)
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: "caller"}, nil
	}
	for i, target := range []string{"/target", "/updated-target"} {
		require.NoError(t, d.Set("database_name", target))
		operation := eds.ResourceCreateFunc(cb)
		if i > 0 {
			operation = eds.ResourceUpdateFunc(cb)
		}
		diags := operation(t.Context(), d, nil)
		require.False(t, diags.HasError(), "%v", diags)
		require.NotEmpty(t, d.Id())
		assert.NotContains(t, d.State().Attributes, "resource_id", "server-resolved cloud IDs are not Terraform attributes")
		f.mu.Lock()
		assert.Equal(t, "resolved:"+target, f.properties["RESOURCE_ID"])
		for _, req := range f.requests {
			q := req.query.GetQueryContent().GetText()
			if strings.Contains(q, "EXTERNAL DATA SOURCE") {
				assert.NotContains(t, q, "RESOURCE_ID", "every IAM mutation must let the server resolve the target cloud ID")
			}
		}
		f.mu.Unlock()
	}
}

func TestIAMDatabaseTargetPlans(t *testing.T) {
	for _, tc := range []struct {
		name, databaseName string
		wantError          bool
	}{
		{name: "full path", databaseName: "/target"},
		{name: "blank path", databaseName: "  ", wantError: true},
		{name: "relative path", databaseName: "target", wantError: true},
		{name: "unknown path", databaseName: "74D93920-ED26-11E3-AC10-0800200C9A66"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := ydbprovider.Provider().ResourcesMap["ydb_external_data_source"]
			cfg := iamResourceConfig(&iamWireDB{addr: "localhost:2136"})
			cfg["database_name"] = tc.databaseName
			cfg["database_id"] = "target-id"
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
			r := ydbprovider.Provider().ResourcesMap["ydb_external_data_source"]
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
	return r.Diff(context.Background(), state, terraform.NewResourceConfigRaw(cfg), nil)
}
