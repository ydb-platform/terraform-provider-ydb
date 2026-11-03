package externaldatasource

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The break caught here is relying on SetId + Error to preserve an untainted EDS.
// This intentionally characterizes Core with a tiny fixture, not a cloud mock.
func TestLocalTerraformIAMCreateErrorTaints(t *testing.T) {
	cli := newTerraformCLI(t)
	dir := cli.dir
	plugins := filepath.Join(dir, "plugins")
	buildTestProvider(t, testProviderBinary(t, plugins), "./testdata/error-provider")
	run := cli.run
	require.NoError(t, os.WriteFile(filepath.Join(dir, "main.tf"), []byte(`terraform {
 required_providers {
 test = { source = "example/test", version = "0.0.1" }
 }
}
resource "test_eds" "example" { path = "eds" }
`), 0o600))
	out, err := run("init", "-plugin-dir="+plugins, "-no-color")
	require.NoError(t, err, out)
	out, err = run("apply", "-auto-approve", "-no-color")
	require.Error(t, err)
	require.Contains(t, out, "bootstrap cleanup failed")
	raw, err := os.ReadFile(filepath.Join(dir, "terraform.tfstate"))
	require.NoError(t, err)
	var st struct {
		Resources []struct {
			Instances []struct {
				Status     string
				Attributes map[string]interface{}
			}
		}
	}
	require.NoError(t, json.Unmarshal(raw, &st))
	require.Len(t, st.Resources, 1)
	require.Len(t, st.Resources[0].Instances, 1)
	instance := st.Resources[0].Instances[0]
	require.Equal(t, "tainted", instance.Status)
	require.Equal(t, "confirmed-eds", instance.Attributes["id"])
	require.Equal(t, "/owner/confirmed-orphan", instance.Attributes["pending_cleanup_path"])
	out, err = run("plan", "-no-color")
	require.NoError(t, err, out)
	require.True(t, strings.Contains(out, "must be replaced"), out)
	t.Log("Terraform persisted confirmed identity + pending cleanup as tainted; next plan replaces EDS")
}
