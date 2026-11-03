package externaldatasource

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

// Client initialization failures can contain the token even before SQL is sent.
// Both operations must preserve identity and report status without that content.
func TestYDBIAMReadDeleteClientErrorRedaction(t *testing.T) {
	const sentinel = "credential-sentinel-'\\\n-for-client-open"
	for _, operation := range []string{"Read", "Delete"} {
		t.Run(operation, func(t *testing.T) {
			f := &fakeBackend{}
			h := existingHandler(f)
			d := existingData(t)
			id := "grpc://localhost:2136/?database=/owner?path=eds"
			d.SetId(id)
			h.openBackend = func(context.Context, string, auth.YdbCredentials) (backend, error) {
				return nil, status.Error(codes.Unauthenticated, "credentials="+sentinel)
			}
			var call func(context.Context, *schema.ResourceData, interface{}) diag.Diagnostics
			if operation == "Read" {
				call = h.Read
			} else {
				call = h.Delete
			}
			diags := call(context.Background(), d, nil)
			require.True(t, diags.HasError())
			require.Equal(t, id, d.Id())
			require.Empty(t, f.queries)
			require.False(t, strings.Contains(fmt.Sprint(diags), sentinel), "client error exposed credential in diagnostics")
			require.Contains(t, fmt.Sprint(diags), "status 16")
		})
	}
}
