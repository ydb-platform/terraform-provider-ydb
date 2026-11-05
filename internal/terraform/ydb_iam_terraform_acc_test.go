package terraform_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const iamTestProviderAddress = "terraform.storage.ydb.tech/provider/ydb"

func TestAccYdbExternalDataSource_iamLifecycle(t *testing.T) {
	cli := newIAMTerraform(t)
	f := newIAMWireDB(t)
	const (
		address = "ydb_external_data_source.test"
		token   = "synthetic-terraform-caller-token"
	)
	config := func(serviceAccount string, extra ...string) {
		cli.write("main.tf", fmt.Sprintf(`terraform {
  required_providers { ydb = { source = %q } }
}
provider "ydb" { token = %q }
resource "ydb_external_data_source" "test" {
  connection_string = "grpc://%s/?database=/owner"
  path = "eds"
  source_type = "Ydb"
  location = "target:2135"
  database_name = "/target"
  auth_method = "IAM"
  service_account_id = %q
  use_tls = false
  %s
}
`, iamTestProviderAddress, token, f.addr, serviceAccount, strings.Join(extra, "\n")))
	}
	run := func(args ...string) {
		t.Helper()
		out, err := cli.run(args...)
		require.NoError(t, err, "%s", out)
	}
	checkState := func(serviceAccount string) map[string]interface{} {
		t.Helper()
		attrs := cli.state(address)
		require.Equal(t, "IAM", attrs["auth_method"])
		require.Equal(t, serviceAccount, attrs["service_account_id"])
		for _, key := range []string{"token", "iam_bootstrap_mode", "initial_token_secret_path"} {
			require.NotContains(t, attrs, key)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		require.True(t, f.exists)
		require.Equal(t, serviceAccount, f.properties["SERVICE_ACCOUNT_ID"])
		require.Empty(t, f.secrets)
		return attrs
	}
	config("runtime-sa")
	run("apply", "-auto-approve", "-no-color")
	id := checkState("runtime-sa")["id"].(string)
	require.NotEmpty(t, id)
	run("plan", "-detailed-exitcode", "-no-color")
	for _, argument := range []string{"iam_bootstrap_mode", "initial_token_secret_path"} {
		config("runtime-sa", argument+` = "external"`)
		out, err := cli.run("plan", "-no-color")
		require.Error(t, err)
		require.Contains(t, out, "Unsupported argument")
		require.Contains(t, out, argument)
	}
	config("updated-runtime-sa")
	run("apply", "-auto-approve", "-no-color")
	require.Equal(t, id, checkState("updated-runtime-sa")["id"])
	run("plan", "-detailed-exitcode", "-no-color")
	run("state", "rm", address)
	run("import", "-no-color", address, id)
	require.Equal(t, id, checkState("updated-runtime-sa")["id"])
	run("plan", "-detailed-exitcode", "-no-color")
	run("destroy", "-auto-approve", "-no-color")
	f.mu.Lock()
	require.False(t, f.exists)
	require.Empty(t, f.secrets)
	f.mu.Unlock()
	cli.assertNoToken(token)
}

type iamTerraform struct {
	t          *testing.T
	executable string
	dir        string
	env        []string
	outputs    []string
}

func newIAMTerraform(t *testing.T) *iamTerraform {
	t.Helper()
	if os.Getenv("TF_ACC") != "1" {
		t.Skip("set TF_ACC=1 to run Terraform acceptance tests")
	}
	executable := os.Getenv("TF_ACC_TERRAFORM_PATH")
	if executable == "" {
		var err error
		executable, err = exec.LookPath("terraform")
		require.NoError(t, err, "Terraform CLI required; set TF_ACC_TERRAFORM_PATH")
	}
	cli := &iamTerraform{t: t, executable: executable, dir: t.TempDir()}
	plugins := t.TempDir()
	binary := filepath.Join(plugins, "terraform-provider-ydb")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-buildvcs=false", "-o", binary, "../..")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	cli.write("terraform.tfrc", fmt.Sprintf(`provider_installation {
  dev_overrides { %q = %q }
  direct {}
}
`, iamTestProviderAddress, plugins))
	cli.env = []string{
		"TF_IN_AUTOMATION=1", "CHECKPOINT_DISABLE=1",
		"TF_CLI_CONFIG_FILE=" + filepath.Join(cli.dir, "terraform.tfrc"),
		"TF_LOG=OFF", "TF_LOG_SDK=OFF",
	}
	return cli
}

func (cli *iamTerraform) write(name, content string) {
	cli.t.Helper()
	require.NoError(cli.t, os.WriteFile(filepath.Join(cli.dir, name), []byte(content), 0o600))
}

// run uses the test runner's timeout, with no independent request deadline.
func (cli *iamTerraform) run(args ...string) (string, error) {
	cli.t.Helper()
	//nolint:gosec // The executable is a local test dependency, with arguments supplied by tests.
	cmd := exec.CommandContext(cli.t.Context(), cli.executable, args...)
	cmd.Dir = cli.dir
	cmd.Env = append(os.Environ(), cli.env...)
	out, err := cmd.CombinedOutput()
	cli.outputs = append(cli.outputs, string(out))
	return string(out), err
}

func (cli *iamTerraform) state(address string) map[string]interface{} {
	cli.t.Helper()
	raw, err := os.ReadFile(filepath.Join(cli.dir, "terraform.tfstate"))
	require.NoError(cli.t, err)
	var state struct {
		Resources []struct {
			Mode      string `json:"mode"`
			Type      string `json:"type"`
			Name      string `json:"name"`
			Instances []struct {
				Attributes map[string]interface{} `json:"attributes"`
			} `json:"instances"`
		} `json:"resources"`
	}
	require.NoError(cli.t, json.Unmarshal(raw, &state))
	for _, resource := range state.Resources {
		name := resource.Type + "." + resource.Name
		if resource.Mode == "data" {
			name = "data." + name
		}
		if name == address {
			require.Len(cli.t, resource.Instances, 1)
			return resource.Instances[0].Attributes
		}
	}
	cli.t.Fatalf("resource %q absent from Terraform state", address)
	return nil
}

// assertNoToken checks provider outputs and persisted state/logs. Input HCL and
// saved plans can contain caller-supplied credentials, so they are not scanned.
func (cli *iamTerraform) assertNoToken(token string) {
	cli.t.Helper()
	for _, output := range cli.outputs {
		require.NotContains(cli.t, output, token, "caller token leaked to CLI output")
	}
	entries, err := os.ReadDir(cli.dir)
	require.NoError(cli.t, err)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || (!strings.Contains(name, ".tfstate") && !strings.HasSuffix(name, ".log")) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(cli.dir, name))
		require.NoError(cli.t, err)
		require.NotContains(cli.t, string(raw), token, "caller token leaked to %s", name)
	}
}
