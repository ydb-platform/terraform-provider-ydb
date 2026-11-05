package terraform_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccYdbExternalDataSource_iamCleanupFailureRequiresManualCleanup(t *testing.T) {
	cli := newIAMTerraform(t)
	f := newIAMWireDB(t)
	f.failDrop = true
	const address = "ydb_external_data_source.test"
	writeIAMRecoveryConfig(cli, f, "runtime-sa")
	out, err := cli.run("apply", "-auto-approve", "-no-color")
	require.NoError(t, err, "%s", out)
	require.Contains(t, out, "temporary secret cleanup pending")
	require.Contains(t, out, "Remove the owned secret manually.")
	attrs := cli.state(address)
	id := attrs["id"]
	require.NotEmpty(t, id)
	require.Equal(t, "IAM", attrs["auth_method"], "cleanup warnings must still complete read-back")
	for _, removed := range []string{"resource_id", "pending_cleanup_path", "pending_iam_mutation"} {
		require.NotContains(t, attrs, removed)
	}
	f.mu.Lock()
	require.Len(t, f.secrets, 1)
	secretPath := f.properties["INITIAL_TOKEN_SECRET_PATH"]
	require.Contains(t, out, secretPath)
	f.failDrop = false
	requests := len(f.requests)
	f.mu.Unlock()
	// Each CLI invocation starts a new provider process and reloads the saved state.
	out, err = cli.run("apply", "-refresh-only", "-auto-approve", "-no-color")
	require.NoError(t, err, "%s", out)
	attrs = cli.state(address)
	require.Equal(t, id, attrs["id"])
	f.mu.Lock()
	require.Len(t, f.secrets, 1)
	require.Len(t, f.requests, requests, "refresh must not retry cleanup")
	delete(f.secrets, secretPath) // Simulate manual cleanup of the exact owned path from the warning.
	f.mu.Unlock()
	out, err = cli.run("plan", "-detailed-exitcode", "-no-color")
	require.NoError(t, err, "%s", out)
	cli.assertNoToken("synthetic-recovery-caller")
}

func TestAccYdbExternalDataSource_iamUnknownCreateDoesNotAcquireOwnership(t *testing.T) {
	cli := newIAMTerraform(t)
	f := newIAMWireDB(t)
	f.loseReply = "EXTERNAL DATA SOURCE"
	writeIAMRecoveryConfig(cli, f, "runtime-sa")
	out, err := cli.run("apply", "-auto-approve", "-no-color")
	require.Error(t, err, "%s", out)
	require.Contains(t, out, "outcome is unknown")
	out, err = cli.run("state", "list")
	require.NoError(t, err, "%s", out)
	require.NotContains(t, out, "ydb_external_data_source.test")
	f.mu.Lock()
	require.True(t, f.exists, "the server accepted the request before losing its response")
	require.Len(t, f.secrets, 1, "the unacknowledged operation may still need its bootstrap secret")
	f.mu.Unlock()
	cli.assertNoToken("synthetic-recovery-caller")
}

func writeIAMRecoveryConfig(cli *iamTerraform, f *iamWireDB, serviceAccount string) {
	cli.t.Helper()
	cli.write("main.tf", strings.NewReplacer(
		"127.0.0.1:2136", f.addr,
		`"runtime-sa"`, strconv.Quote(serviceAccount),
	).Replace(`terraform {
  required_providers { ydb = { source = "terraform.storage.ydb.tech/provider/ydb" } }
}
provider "ydb" { token = "synthetic-recovery-caller" }
resource "ydb_external_data_source" "test" {
  connection_string = "grpc://127.0.0.1:2136/?database=/owner"
  path = "eds"
  source_type = "Ydb"
  location = "target:2135"
  database_name = "/target"
  auth_method = "IAM"
  service_account_id = "runtime-sa"
}
`))
}
