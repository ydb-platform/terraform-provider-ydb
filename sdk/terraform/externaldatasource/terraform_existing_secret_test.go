package externaldatasource

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// With the optional baseline binary, real Terraform Core verifies that upgrading
// an existing-secret resource preserves mode and ownership. Without it the same
// checks exercise the candidate's complete standalone create/import lifecycle.
func TestLocalTerraformYDBExistingSecretLifecycle(t *testing.T) {
	cli := newTerraformCLI(t)
	f := newWireDB(t)
	f.secrets["/owner/manual"] = true
	next := filepath.Join(cli.dir, "terraform-provider-ydb-next")
	buildTestProvider(t, next, "../../..")
	baseline := os.Getenv("YDB_TEST_EXISTING_SECRET_PROVIDER_BINARY")
	if baseline == "" {
		baseline = next
	}
	require.FileExists(t, baseline)
	plugins := filepath.Join(cli.dir, "provider")
	require.NoError(t, os.MkdirAll(plugins, 0o700))
	active := filepath.Join(plugins, "terraform-provider-ydb")
	cliConfig := filepath.Join(cli.dir, "terraform.tfrc")
	require.NoError(t, os.WriteFile(cliConfig, []byte(fmt.Sprintf(`provider_installation {
 dev_overrides { "terraform.storage.ydb.tech/provider/ydb" = %q }
 direct {}
}
`, plugins)), 0o600))
	cli.env = []string{"TF_CLI_CONFIG_FILE=" + cliConfig, "TF_LOG=OFF", "TF_LOG_SDK=OFF"}
	config := func(location string) {
		require.NoError(t, os.WriteFile(filepath.Join(cli.dir, "main.tf"), []byte(fmt.Sprintf(`terraform {
 required_providers { ydb = { source = "terraform.storage.ydb.tech/provider/ydb" } }
}
provider "ydb" { token = "existing-secret-transport" }
resource "ydb_external_data_source" "manual" {
 connection_string = "grpc://%s/?database=/owner"
 path = "eds"
 source_type = "Ydb"
 location = %q
 auth_method = "IAM"
 service_account_id = "sa"
 database_id = "target"
 initial_token_secret_path = "/owner/manual"
 use_tls = false
}
`, f.addr, location)), 0o600))
	}
	switchProvider := func(binary string) { _ = os.Remove(active); require.NoError(t, os.Symlink(binary, active)) }
	state := func() map[string]interface{} {
		raw, err := os.ReadFile(filepath.Join(cli.dir, "terraform.tfstate"))
		require.NoError(t, err)
		var doc struct {
			Resources []struct {
				Instances []struct {
					Attributes map[string]interface{} `json:"attributes"`
				} `json:"instances"`
			} `json:"resources"`
		}
		require.NoError(t, json.Unmarshal(raw, &doc))
		require.Len(t, doc.Resources, 1)
		require.Len(t, doc.Resources[0].Instances, 1)
		attrs := doc.Resources[0].Instances[0].Attributes
		require.Equal(t, "EXISTING_SECRET", attrs["iam_bootstrap_mode"])
		require.Equal(t, "/owner/manual", attrs["initial_token_secret_path"])
		require.Empty(t, attrs["pending_iam_mutation"])
		require.Empty(t, attrs["pending_cleanup_path"])
		return attrs
	}
	run := func(args ...string) { out, err := cli.run(args...); require.NoError(t, err, out) }
	config("target:2135")
	switchProvider(baseline)
	run("apply", "-auto-approve", "-no-color")
	id := state()["id"].(string)
	run("plan", "-detailed-exitcode", "-no-color")
	switchProvider(next)
	run("plan", "-detailed-exitcode", "-no-color")
	config("updated:2135")
	run("apply", "-auto-approve", "-no-color")
	require.Equal(t, id, state()["id"])
	require.Equal(t, "updated:2135", state()["location"])
	run("plan", "-detailed-exitcode", "-no-color")
	run("state", "rm", "ydb_external_data_source.manual")
	run("import", "-no-color", "ydb_external_data_source.manual", id)
	require.Equal(t, id, state()["id"])
	run("plan", "-detailed-exitcode", "-no-color")
	config("after-import:2135")
	run("apply", "-auto-approve", "-no-color")
	require.Equal(t, id, state()["id"])
	// The supported manual-secret mode also survives rollback to Stage A.
	switchProvider(baseline)
	run("plan", "-detailed-exitcode", "-no-color")
	run("apply", "-auto-approve", "-no-color")
	require.Equal(t, id, state()["id"])
	switchProvider(next)
	run("plan", "-detailed-exitcode", "-no-color")
	run("destroy", "-auto-approve", "-no-color")
	f.mu.Lock()
	defer f.mu.Unlock()
	require.False(t, f.exists)
	require.Equal(t, map[string]bool{"/owner/manual": true}, f.secrets, "externally owned secret must survive every operation")
	var creates, replaces int
	for _, q := range f.queries {
		require.NotContains(t, q, "CREATE SECRET")
		require.NotContains(t, q, "DROP SECRET")
		require.NotContains(t, q, "__tf_iam_")
		if strings.HasPrefix(q, "CREATE EXTERNAL DATA SOURCE") {
			creates++
		}
		if strings.HasPrefix(q, "CREATE OR REPLACE EXTERNAL DATA SOURCE") {
			replaces++
		}
	}
	require.Equal(t, 1, creates)
	require.Equal(t, 2, replaces)
	t.Logf("real Terraform create (%s) -> candidate no-op/update/import/update -> baseline rollback -> candidate destroy; mode, EDS identity and external secret ownership retained", baseline)
}
