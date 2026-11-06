package terraform_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	eds "github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/externaldatasource"
)

func TestIAMSDKDebugLogs(t *testing.T) {
	const token = "synthetic-sdk-debug-caller-token"
	if os.Getenv("TF_YDB_IAM_LOG_TEST_CHILD") == "1" {
		f := newIAMWireDB(t)
		d := iamWireData(t, f)
		cb := func(context.Context) (auth.YdbCredentials, error) {
			return auth.YdbCredentials{Token: token}, nil
		}
		require.False(t, eds.ResourceCreateFunc(cb)(t.Context(), d, nil).HasError())
		f.mu.Lock()
		f.echoSecretError = true
		f.mu.Unlock()
		for _, operation := range []helpers.TerraformCRUD{eds.ResourceUpdateFunc(cb), eds.ResourceCreateFunc(cb)} {
			diags := operation(t.Context(), d, nil)
			require.True(t, diags.HasError(), "server must reject CREATE SECRET")
			require.False(t, strings.Contains(fmt.Sprint(diags), token), "caller token leaked to diagnostics")
		}
		return
	}
	//nolint:gosec // Reexecute this test binary to isolate SDK logging and capture stderr.
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestIAMSDKDebugLogs$")
	cmd.Env = append(os.Environ(), "TF_YDB_IAM_LOG_TEST_CHILD=1", "YDB_LOG_SEVERITY_LEVEL=trace", "YDB_LOG_DETAILS=.*", "YDB_ACC_IAM_ACCESS_SERVICE_ADDR=")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "SDK logging probe failed")
	require.False(t, strings.Contains(string(out), token), "caller token leaked to SDK stderr with TRACE enabled")
}
