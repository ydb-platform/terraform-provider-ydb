package terraform

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/stretchr/testify/require"
)

func TestExternalDataSourceCallerTokenPlans(t *testing.T) {
	for _, tc := range []struct {
		name       string
		authMethod string
		token      string
		wantError  bool
	}{
		{name: "IAM without token", authMethod: "IAM", wantError: true},
		{name: "IAM with token", authMethod: "IAM", token: "caller-token"},
		{name: "non-IAM without token", authMethod: "NONE"},
		{name: "unknown auth without token", authMethod: "74D93920-ED26-11E3-AC10-0800200C9A66"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := Provider()
			diags := provider.Configure(context.Background(), terraform.NewResourceConfigRaw(map[string]interface{}{
				"user": "caller", "token": tc.token,
			}))
			require.False(t, diags.HasError(), "%v", diags)

			cfg := map[string]interface{}{
				"connection_string": "grpc://localhost:2136/?database=/owner",
				"path":              "eds",
				"source_type":       "Ydb",
				"location":          "target:2135",
				"database_name":     "/target",
				"auth_method":       tc.authMethod,
			}
			if tc.authMethod != "NONE" {
				cfg["service_account_id"] = "runtime-sa"
			}
			_, err := provider.ResourcesMap["ydb_external_data_source"].Diff(
				context.Background(), &terraform.InstanceState{},
				terraform.NewResourceConfigRaw(cfg), provider.Meta(),
			)
			if tc.wantError {
				require.ErrorContains(t, err, "IAM authentication requires the caller's IAM token")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestExternalDataSourceSharedReadingPlans(t *testing.T) {
	for _, sourceType := range []string{"Ydb", "YdbTopics", "ObjectStorage"} {
		for _, tc := range []struct {
			name  string
			value interface{}
		}{
			{name: "enabled", value: true},
			{name: "disabled explicitly", value: false},
			{name: "omitted"},
		} {
			t.Run(sourceType+"/"+tc.name, func(t *testing.T) {
				cfg := map[string]interface{}{
					"connection_string": "grpc://localhost:2136/?database=/owner",
					"path":              "eds",
					"source_type":       sourceType,
					"location":          "target:2135",
				}
				if sourceType == "Ydb" {
					cfg["database_name"] = "/target"
				}
				if tc.value != nil {
					cfg["shared_reading"] = tc.value
				}
				_, err := ydbExternalDataSourceResource().Diff(
					t.Context(), &terraform.InstanceState{}, terraform.NewResourceConfigRaw(cfg), nil,
				)
				if sourceType == "ObjectStorage" && tc.value != nil {
					require.ErrorContains(t, err, "SHARED_READING is not supported")
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}
