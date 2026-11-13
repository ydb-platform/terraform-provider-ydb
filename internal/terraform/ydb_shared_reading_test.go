package terraform_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	eds "github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/externaldatasource"
)

func TestExternalDataSourceSharedReadingCreate(t *testing.T) {
	f := newIAMWireDB(t)
	resourceSchema := eds.ResourceSchema()
	d := schema.TestResourceDataRaw(t, resourceSchema, map[string]interface{}{
		"connection_string": fmt.Sprintf("grpc://%s/?database=/owner", f.addr),
		"path":              "eds",
		"source_type":       "Ydb",
		"location":          "target:2135",
		"database_name":     "/target",
		"auth_method":       "NONE",
		"shared_reading":    true,
	})
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{}, nil
	}

	diags := eds.ResourceCreateFunc(cb)(t.Context(), d, nil)

	require.False(t, diags.HasError(), "%v", diags)
	require.NotEmpty(t, d.Id())
	f.mu.Lock()
	defer f.mu.Unlock()
	require.True(t, f.exists)
	require.Equal(t, "TRUE", f.properties["SHARED_READING"])
	require.Equal(t, true, d.Get("shared_reading"))
}

func TestExternalDataSourceSharedReadingRead(t *testing.T) {
	f := newIAMWireDB(t)
	f.exists = true
	f.properties = map[string]string{
		"SOURCE_TYPE":    "Ydb",
		"LOCATION":       "target:2135",
		"DATABASE_NAME":  "/target",
		"AUTH_METHOD":    "NONE",
		"SHARED_READING": "TRUE",
	}
	d := schema.TestResourceDataRaw(t, eds.DataSourceSchema(), map[string]interface{}{
		"connection_string": fmt.Sprintf("grpc://%s/?database=/owner", f.addr),
		"path":              "eds",
	})
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{}, nil
	}

	diags := eds.DataSourceReadFunc(cb)(t.Context(), d, nil)

	require.False(t, diags.HasError(), "%v", diags)
	require.NotEmpty(t, d.Id())
	require.Equal(t, true, d.Get("shared_reading"))
}
