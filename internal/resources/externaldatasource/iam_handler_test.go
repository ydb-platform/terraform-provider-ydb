package externaldatasource

import (
	"context"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func existingData(t *testing.T) *schema.ResourceData {
	t.Helper()
	s := flattenTestSchema()
	for _, k := range []string{"iam_bootstrap_mode", "initial_token_secret_path", "resource_id", "pending_iam_mutation"} {
		s[k] = &schema.Schema{Type: schema.TypeString, Optional: true}
	}
	return schema.TestResourceDataRaw(t, s, map[string]interface{}{"connection_string": "grpc://localhost:2136/?database=/owner", "path": "eds", "source_type": "Ydb", "location": "target:2135", "auth_method": "IAM", "service_account_id": "runtime-sa", "database_id": "target", "iam_bootstrap_mode": "EXISTING_SECRET", "initial_token_secret_path": "/owner/existing"})
}

func existingHandler(f *fakeBackend) *Handler {
	h := NewHandler(auth.YdbCredentials{Token: "transport"})
	h.openBackend = func(context.Context, string, auth.YdbCredentials) (backend, error) { return f, nil }
	return h
}

func existingDescription() options.ExternalDataSourceDescription {
	return options.ExternalDataSourceDescription{SourceType: "Ydb", Location: "target:2135", Properties: map[string]string{"AUTH_METHOD": "IAM", "SERVICE_ACCOUNT_ID": "runtime-sa", "DATABASE_ID": "target", "INITIAL_TOKEN_SECRET_PATH": "/owner/existing", "RESOURCE_ID": "cloud"}}
}

func TestExistingSecretLifecycle(t *testing.T) {
	f := &fakeBackend{desc: existingDescription()}
	h, d := existingHandler(f), existingData(t)
	require.False(t, h.Create(context.Background(), d, nil).HasError())
	id := d.Id()
	require.NotEmpty(t, id)
	require.False(t, h.Update(context.Background(), d, nil).HasError())
	require.Equal(t, id, d.Id())
	require.Len(t, f.queries, 2)
	require.Contains(t, f.queries[0], "CREATE EXTERNAL DATA SOURCE")
	require.Contains(t, f.queries[1], "CREATE OR REPLACE EXTERNAL DATA SOURCE")
	require.NoError(t, d.Set("iam_bootstrap_mode", ""))
	require.False(t, h.Read(context.Background(), d, nil).HasError())
	require.Equal(t, "EXISTING_SECRET", d.Get("iam_bootstrap_mode"))
	require.False(t, h.Delete(context.Background(), d, nil).HasError())
	require.Len(t, f.queries, 3)
	for _, q := range f.queries {
		require.NotContains(t, q, "CREATE SECRET")
		require.NotContains(t, q, "DROP SECRET")
	}
}

func TestExistingSecretMutationOutcomes(t *testing.T) {
	for _, code := range []codes.Code{codes.AlreadyExists, codes.PermissionDenied, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			f := &fakeBackend{exec: func(context.Context, string, []query.ExecuteOption) error {
				return status.Error(code, "credential=sentinel")
			}}
			h, d := existingHandler(f), existingData(t)
			diags := h.Create(context.Background(), d, nil)
			require.True(t, diags.HasError())
			require.Empty(t, d.Id())
			require.NotContains(t, diags[0].Summary, "sentinel")
			require.Zero(t, f.describeCalls)
		})
	}
}

func TestExistingSecretUnknownUpdateBlocksFurtherOperations(t *testing.T) {
	f := &fakeBackend{desc: existingDescription()}
	f.exec = func(_ context.Context, q string, _ []query.ExecuteOption) error {
		if strings.Contains(q, "EXTERNAL DATA SOURCE") {
			return status.Error(codes.DeadlineExceeded, "response lost")
		}
		return nil
	}
	h := existingHandler(f)
	d := existingData(t)
	d.SetId("grpc://localhost:2136/?database=/owner?path=eds")
	require.NoError(t, d.Set("iam_bootstrap_mode", "EXISTING_SECRET"))
	require.NoError(t, d.Set("initial_token_secret_path", "/owner/existing"))
	require.True(t, h.Update(context.Background(), d, nil).HasError())
	require.Equal(t, "UNKNOWN", d.Get("pending_iam_mutation"))
	before := len(f.queries)
	require.True(t, h.Read(context.Background(), d, nil).HasError())
	require.True(t, h.Update(context.Background(), d, nil).HasError())
	require.True(t, h.Delete(context.Background(), d, nil).HasError())
	require.Len(t, f.queries, before)
}
