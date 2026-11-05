package externaldatasource

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIAMTerraformCleanupRecovery(t *testing.T) {
	cli := newIAMTerraform(t)
	f := newIAMWireDB(t)
	f.failDrop = true
	const address = "ydb_external_data_source.test"
	writeIAMRecoveryConfig(cli, f, "runtime-sa")
	out, err := cli.run("apply", "-auto-approve", "-no-color")
	require.NoError(t, err, "%s", out)
	attrs := cli.state(address)
	id := attrs["id"]
	require.NotEmpty(t, id)
	require.Equal(t, "resolved:/target", attrs["resource_id"], "cleanup warnings must still resolve computed attributes")
	require.NotEmpty(t, attrs["pending_cleanup_path"])
	f.mu.Lock()
	f.failDrop = false
	requests := len(f.requests)
	f.mu.Unlock()
	// Each CLI invocation starts a new provider process and reloads the saved state.
	out, err = cli.run("apply", "-refresh-only", "-auto-approve", "-no-color")
	require.NoError(t, err, "%s", out)
	attrs = cli.state(address)
	require.Equal(t, id, attrs["id"])
	require.Empty(t, attrs["pending_cleanup_path"])
	f.mu.Lock()
	require.Empty(t, f.secrets)
	require.Len(t, f.requests, requests+1, "refresh must only remove the saved owned secret")
	f.mu.Unlock()
	out, err = cli.run("plan", "-detailed-exitcode", "-no-color")
	require.NoError(t, err, "%s", out)
	cli.assertNoToken("synthetic-recovery-caller")
}

func TestIAMTerraformUnknownUpdateSurvivesRestart(t *testing.T) {
	cli := newIAMTerraform(t)
	f := newIAMWireDB(t)
	const address = "ydb_external_data_source.test"
	writeIAMRecoveryConfig(cli, f, "runtime-sa")
	out, err := cli.run("apply", "-auto-approve", "-no-color")
	require.NoError(t, err, "%s", out)
	id := cli.state(address)["id"]
	writeIAMRecoveryConfig(cli, f, "updated-runtime-sa")
	f.mu.Lock()
	f.loseReply = "EXTERNAL DATA SOURCE"
	f.mu.Unlock()
	out, err = cli.run("apply", "-auto-approve", "-no-color")
	require.Error(t, err, "%s", out)
	attrs := cli.state(address)
	require.Equal(t, id, attrs["id"])
	require.Equal(t, "UNKNOWN", attrs["pending_iam_mutation"])
	require.NotEmpty(t, attrs["pending_cleanup_path"])
	f.mu.Lock()
	requests := len(f.requests)
	require.Len(t, f.secrets, 1)
	f.mu.Unlock()
	out, err = cli.run("plan", "-no-color")
	require.Error(t, err, "%s", out)
	require.Contains(t, out, "outcome remains unknown")
	f.mu.Lock()
	require.Len(t, f.requests, requests, "a new provider must honor the durable guard")
	// Simulate an operator verifying that the accepted request has terminated,
	// reconciling EDS ownership and removing exactly its temporary secret.
	require.Equal(t, "updated-runtime-sa", f.properties["SERVICE_ACCOUNT_ID"])
	delete(f.secrets, attrs["pending_cleanup_path"].(string))
	f.mu.Unlock()
	out, err = cli.run("state", "rm", address)
	require.NoError(t, err, "%s", out)
	out, err = cli.run("import", "-no-color", address, id.(string))
	require.NoError(t, err, "%s", out)
	require.Empty(t, cli.state(address)["pending_iam_mutation"])
	out, err = cli.run("plan", "-detailed-exitcode", "-no-color")
	require.NoError(t, err, "%s", out)
	cli.assertNoToken("synthetic-recovery-caller")
}

func TestIAMTerraformUnknownCreateDoesNotAcquireOwnership(t *testing.T) {
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
	cli.write("main.tf", fmt.Sprintf(`terraform {
  required_providers { ydb = { source = %q } }
}
provider "ydb" { token = "synthetic-recovery-caller" }
resource "ydb_external_data_source" "test" {
  connection_string = "grpc://%s/?database=/owner"
  path = "eds"
  source_type = "Ydb"
  location = "target:2135"
  database_name = "/target"
  auth_method = "IAM"
  service_account_id = %q
}
`, iamTestProviderAddress, f.addr, serviceAccount))
}
