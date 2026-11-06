package terraform_test

import (
	"context"
	"errors"
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

func TestIAMCredentialErrorsPreserveDetails(t *testing.T) {
	wantErr := errors.New("credential service is unavailable (sentinel)")
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{}, wantErr
	}
	for name, operation := range map[string]helpers.TerraformCRUD{
		"create": eds.ResourceCreateFunc(cb), "read": eds.ResourceReadFunc(cb), "update": eds.ResourceUpdateFunc(cb),
		"delete": eds.ResourceDeleteFunc(cb), "data-source": eds.DataSourceReadFunc(cb),
	} {
		t.Run(name, func(t *testing.T) {
			d := schema.TestResourceDataRaw(t, eds.ResourceSchema(), nil)
			diags := operation(context.Background(), d, nil)
			require.True(t, diags.HasError())
			require.Len(t, diags, 1)
			require.Equal(t, wantErr.Error(), diags[0].Detail)
		})
	}
}

func TestIAMServerErrorsPreserveDetails(t *testing.T) {
	const detail = "request rejected: permission sentinel"
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: "caller"}, nil
	}
	for _, operation := range []string{"connect", "create-secret", "create", "update", "read", "delete", "cleanup", "read-back"} {
		t.Run(operation, func(t *testing.T) {
			f := newIAMWireDB(t)
			d := iamWireData(t, f)
			if operation == "update" || operation == "read" || operation == "delete" {
				require.False(t, eds.ResourceCreateFunc(cb)(context.Background(), d, nil).HasError())
			}
			failure := status.Error(codes.PermissionDenied, detail)
			f.mu.Lock()
			switch operation {
			case "connect":
				f.discoveryError = failure
			case "create-secret":
				f.secretError = failure
			case "create", "update":
				f.mutationError = failure
			case "read", "read-back":
				f.describeError = failure
			case "delete":
				f.deleteError = failure
			case "cleanup":
				f.dropError = failure
			}
			f.mu.Unlock()
			crud := eds.ResourceCreateFunc(cb)
			switch operation {
			case "update":
				crud = eds.ResourceUpdateFunc(cb)
			case "read":
				crud = eds.ResourceReadFunc(cb)
			case "delete":
				crud = eds.ResourceDeleteFunc(cb)
			}
			diags := crud(context.Background(), d, nil)
			require.Equal(t, operation != "cleanup", diags.HasError(), "%v", diags)
			require.Contains(t, fmt.Sprint(diags), detail)
		})
	}
}
