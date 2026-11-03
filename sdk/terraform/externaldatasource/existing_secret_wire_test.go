package externaldatasource

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"
)

func TestExistingSecretWireCollision(t *testing.T) {
	f := newWireDB(t)
	f.exists = true
	f.properties = map[string]string{"SOURCE_TYPE": "Ydb", "LOCATION": "foreign", "AUTH_METHOD": "NONE", "DATABASE_ID": "target"}
	cfg := iamConfig()
	cfg["iam_bootstrap_mode"] = "EXISTING_SECRET"
	cfg["initial_token_secret_path"] = "/owner/manual"
	cfg["connection_string"] = fmt.Sprintf("grpc://%s/?database=/owner", f.addr)
	d := schema.TestResourceDataRaw(t, ResourceSchema(), cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.True(t, ResourceCreateFunc(wireCallback)(ctx, d, nil).HasError())
	require.Empty(t, d.Id())
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, "foreign", f.properties["LOCATION"])
	require.Len(t, f.queries, 1)
	require.True(t, strings.HasPrefix(f.queries[0], "CREATE EXTERNAL DATA SOURCE"))
}

// Exercise actual SDK non-idempotent query submission: a lost Update reply must
// not cause a retry or allow subsequent operations to race the original request.
func TestExistingSecretWireUnknownUpdate(t *testing.T) {
	f := newWireDB(t)
	cfg := iamConfig()
	cfg["iam_bootstrap_mode"] = "EXISTING_SECRET"
	cfg["initial_token_secret_path"] = "/owner/manual"
	cfg["connection_string"] = fmt.Sprintf("grpc://%s/?database=/owner", f.addr)
	d := schema.TestResourceDataRaw(t, ResourceSchema(), cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.False(t, ResourceCreateFunc(wireCallback)(ctx, d, nil).HasError())
	id := d.Id()
	f.mu.Lock()
	f.loseReply = "EXTERNAL DATA SOURCE"
	f.mu.Unlock()
	diags := ResourceUpdateFunc(wireCallback)(ctx, d, nil)
	require.True(t, diags.HasError())
	require.Equal(t, id, d.Id())
	require.Equal(t, "UNKNOWN", d.Get("pending_iam_mutation"))
	require.True(t, ResourceReadFunc(wireCallback)(ctx, d, nil).HasError())
	require.True(t, ResourceUpdateFunc(wireCallback)(ctx, d, nil).HasError())
	require.True(t, ResourceDeleteFunc(wireCallback)(ctx, d, nil).HasError())
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Len(t, f.queries, 2, "one Create and exactly one uncertain Update")
	for _, q := range f.queries {
		require.NotContains(t, q, "CREATE SECRET")
		require.NotContains(t, q, "DROP SECRET")
	}
}
