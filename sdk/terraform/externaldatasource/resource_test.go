package externaldatasource

import (
	"context"
	"testing"

	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/stretchr/testify/require"
)

func testResource() *schema.Resource {
	return &schema.Resource{Schema: ResourceSchema(), CustomizeDiff: CustomizeDiff}
}

func iamConfig() map[string]interface{} {
	return map[string]interface{}{"connection_string": "grpc://localhost:2136/?database=/owner", "path": "eds", "source_type": "Ydb", "location": "target:2135", "auth_method": "IAM", "service_account_id": "sa", "database_id": "target", "iam_bootstrap_mode": "CALLER_TOKEN"}
}

func TestIAMResourceSchemaAndDiff(t *testing.T) {
	s := ResourceSchema()
	for _, k := range []string{"iam_bootstrap_mode", "initial_token_secret_path", "resource_id", "pending_cleanup_path"} {
		require.Contains(t, s, k)
	}
	p := &schema.Provider{ResourcesMap: map[string]*schema.Resource{"test_eds": testResource()}}
	// The rest of the methods are supplied just to validate the public schema.
	r := p.ResourcesMap["test_eds"]
	r.CreateContext = schema.CreateContextFunc(ResourceCreateFunc(nil))
	r.ReadContext = schema.ReadContextFunc(ResourceReadFunc(nil))
	r.UpdateContext = schema.UpdateContextFunc(ResourceUpdateFunc(nil))
	r.DeleteContext = schema.DeleteContextFunc(ResourceDeleteFunc(nil))
	require.NoError(t, p.InternalValidate())
	for _, tc := range []struct {
		name string
		vals map[string]interface{}
		bad  bool
	}{
		{"valid", nil, false},
		{"unknown auth", map[string]interface{}{"auth_method": "74D93920-ED26-11E3-AC10-0800200C9A66"}, false},
		{"unknown mode", map[string]interface{}{"iam_bootstrap_mode": "74D93920-ED26-11E3-AC10-0800200C9A66"}, false},
		{"unknown", map[string]interface{}{"service_account_id": "74D93920-ED26-11E3-AC10-0800200C9A66", "database_id": "74D93920-ED26-11E3-AC10-0800200C9A66"}, false},
		{"missing SA", map[string]interface{}{"service_account_id": ""}, true},
		{"explicit caller path", map[string]interface{}{"initial_token_secret_path": "tmp"}, true},
		{"existing missing path", map[string]interface{}{"iam_bootstrap_mode": "EXISTING_SECRET"}, true},
		{"existing unknown path", map[string]interface{}{"iam_bootstrap_mode": "EXISTING_SECRET", "initial_token_secret_path": "74D93920-ED26-11E3-AC10-0800200C9A66"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := iamConfig()
			for k, v := range tc.vals {
				cfg[k] = v
			}
			_, err := iamDiff(r, nil, cfg)
			if tc.bad {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
	for _, k := range []string{"iam_bootstrap_mode", "initial_token_secret_path", "resource_id"} {
		require.True(t, DataSourceSchema()[k].Computed)
	}
}

func TestIAMResourceComputedCloudID(t *testing.T) {
	r := testResource()
	cfg := iamConfig()
	d := schema.TestResourceDataRaw(t, r.Schema, cfg)
	d.SetId("grpc://localhost:2136/?database=/owner?path=eds")
	require.NoError(t, d.Set("resource_id", "old-cloud"))
	require.NoError(t, d.Set("initial_token_secret_path", "/owner/already-dropped"))
	for _, key := range []string{"database_id", "database_name", "location"} {
		t.Run(key, func(t *testing.T) {
			next := iamConfig()
			next[key] = "new-target"
			diff, err := iamDiff(r, d.State(), next)
			require.NoError(t, err)
			require.NotNil(t, diff.Attributes["resource_id"])
			require.True(t, diff.Attributes["resource_id"].NewComputed)
		})
	}
	next := iamConfig()
	next["database_id"] = "new-target"
	next["resource_id"] = "explicit-cloud"
	diff, err := iamDiff(r, d.State(), next)
	require.NoError(t, err)
	require.Equal(t, "explicit-cloud", diff.Attributes["resource_id"].New)
	diff, err = iamDiff(r, d.State(), cfg)
	require.NoError(t, err)
	require.True(t, diff.Empty(), "observed temporary secret must not create drift")
}

func TestIAMCanonicalSecretDiff(t *testing.T) {
	r := testResource()
	cfg := iamConfig()
	cfg["iam_bootstrap_mode"] = "EXISTING_SECRET"
	cfg["initial_token_secret_path"] = "secrets/bootstrap"
	d := schema.TestResourceDataRaw(t, r.Schema, cfg)
	d.SetId("grpc://localhost:2136/?database=/owner?path=eds")
	require.NoError(t, d.Set("initial_token_secret_path", "/owner/secrets/bootstrap"))
	diff, err := iamDiff(r, d.State(), cfg)
	require.NoError(t, err)
	require.True(t, diff.Empty())
}

func iamDiff(r *schema.Resource, state *terraform.InstanceState, cfg map[string]interface{}) (*terraform.InstanceDiff, error) {
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

func TestIAMTransitionClearsComputedMetadata(t *testing.T) {
	r := testResource()
	d := schema.TestResourceDataRaw(t, r.Schema, iamConfig())
	d.SetId("grpc://localhost:2136/?database=/owner?path=eds")
	require.NoError(t, d.Set("initial_token_secret_path", "/owner/dropped"))
	require.NoError(t, d.Set("resource_id", "cloud"))
	cfg := iamConfig()
	cfg["auth_method"] = "NONE"
	delete(cfg, "service_account_id")
	delete(cfg, "iam_bootstrap_mode")
	_, err := iamDiff(r, d.State(), cfg)
	require.NoError(t, err)
}

func TestObjectStorageTLSConfigurationAfterRefresh(t *testing.T) {
	r := testResource()
	cfg := map[string]interface{}{"connection_string": "grpc://localhost:2136/?database=/owner", "path": "eds", "source_type": "ObjectStorage", "location": "bucket", "auth_method": "NONE"}
	d := schema.TestResourceDataRaw(t, r.Schema, cfg)
	d.SetId("grpc://localhost:2136/?database=/owner?path=eds")
	require.NoError(t, d.Set("use_tls", false))
	_, err := iamDiff(r, d.State(), cfg)
	require.NoError(t, err)
	cfg["use_tls"] = false
	_, err = iamDiff(r, d.State(), cfg)
	require.ErrorContains(t, err, "USE_TLS is not supported")
}
