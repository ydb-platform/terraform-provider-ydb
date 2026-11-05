package externaldatasource

import (
	"context"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func TestIAMCredentialErrorsDoNotExposeTokens(t *testing.T) {
	const token = "synthetic-private-caller-token"
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{}, fmt.Errorf("credential service rejected %s", token)
	}
	for name, operation := range map[string]helpers.TerraformCRUD{
		"create": ResourceCreateFunc(cb), "read": ResourceReadFunc(cb), "update": ResourceUpdateFunc(cb),
		"delete": ResourceDeleteFunc(cb), "data-source": DataSourceReadFunc(cb),
	} {
		t.Run(name, func(t *testing.T) {
			d := schema.TestResourceDataRaw(t, ResourceSchema(), nil)
			diags := operation(context.Background(), d, nil)
			require.True(t, diags.HasError())
			require.NotContains(t, fmt.Sprint(diags), token)
		})
	}
}

func TestIAMServerErrorsDoNotExposeTokens(t *testing.T) {
	const token = "synthetic-private-caller-token"
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: token}, nil
	}
	for _, operation := range []string{"connect", "read", "delete"} {
		t.Run(operation, func(t *testing.T) {
			f := newIAMWireDB(t)
			d := iamWireData(t, f)
			if operation != "connect" {
				require.False(t, ResourceCreateFunc(cb)(context.Background(), d, nil).HasError())
			}
			failure := status.Error(codes.PermissionDenied, "request rejected: "+token)
			f.mu.Lock()
			switch operation {
			case "connect":
				f.discoveryError = failure
			case "read":
				f.describeError = failure
			case "delete":
				f.deleteError = failure
			}
			f.mu.Unlock()
			crud := ResourceCreateFunc(cb)
			switch operation {
			case "read":
				crud = ResourceReadFunc(cb)
			case "delete":
				crud = ResourceDeleteFunc(cb)
			}
			diags := crud(context.Background(), d, nil)
			require.True(t, diags.HasError())
			require.NotContains(t, fmt.Sprint(diags), token)
		})
	}
}
