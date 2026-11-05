package terraform_test

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
	eds "github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/externaldatasource"
)

func TestIAMCredentialErrorsDoNotExposeTokens(t *testing.T) {
	const token = "synthetic-private-caller-token"
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{}, fmt.Errorf("credential service rejected %s", token)
	}
	for name, operation := range map[string]helpers.TerraformCRUD{
		"create": eds.ResourceCreateFunc(cb), "read": eds.ResourceReadFunc(cb), "update": eds.ResourceUpdateFunc(cb),
		"delete": eds.ResourceDeleteFunc(cb), "data-source": eds.DataSourceReadFunc(cb),
	} {
		t.Run(name, func(t *testing.T) {
			d := schema.TestResourceDataRaw(t, eds.ResourceSchema(), nil)
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
				require.False(t, eds.ResourceCreateFunc(cb)(context.Background(), d, nil).HasError())
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
			crud := eds.ResourceCreateFunc(cb)
			switch operation {
			case "read":
				crud = eds.ResourceReadFunc(cb)
			case "delete":
				crud = eds.ResourceDeleteFunc(cb)
			}
			diags := crud(context.Background(), d, nil)
			require.True(t, diags.HasError())
			require.NotContains(t, fmt.Sprint(diags), token)
		})
	}
}
