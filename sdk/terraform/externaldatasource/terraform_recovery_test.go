package externaldatasource

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
	"github.com/stretchr/testify/require"
)

// Runs Terraform Core against the actual SDK factories and real YDB network
// adapter. The only remote substitution is the fake gRPC YDB server.
func TestLocalTerraformYDBIAMRecovery(t *testing.T) {
	t.Setenv("TF_LOG_SDK", "OFF")
	t.Setenv("TF_LOG", "OFF")
	cli := newTerraformCLI(t)
	f := newWireDB(t)
	f.failDrop = true
	var dependentCreates, dependentDeletes atomic.Int32
	factory := func() *schema.Provider {
		r := testResource()
		r.CreateContext = schema.CreateContextFunc(ResourceCreateFunc(wireCallback))
		r.ReadContext = schema.ReadContextFunc(ResourceReadFunc(wireCallback))
		r.UpdateContext = schema.UpdateContextFunc(ResourceUpdateFunc(wireCallback))
		r.DeleteContext = schema.DeleteContextFunc(ResourceDeleteFunc(wireCallback))
		r.Importer = &schema.ResourceImporter{StateContext: schema.ImportStatePassthroughContext}
		// A Terraform dependent with ForceNew source identity catches accidental EDS
		// replacement without requiring a second SQL feature in the fake server.
		dependent := &schema.Resource{Schema: map[string]*schema.Schema{"data_source": {Type: schema.TypeString, Required: true, ForceNew: true}}, CreateContext: func(_ context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
			dependentCreates.Add(1)
			d.SetId("dependent-table")
			return nil
		}, ReadContext: func(context.Context, *schema.ResourceData, interface{}) diag.Diagnostics { return nil }, DeleteContext: func(_ context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
			dependentDeletes.Add(1)
			d.SetId("")
			return nil
		}}
		return &schema.Provider{ResourcesMap: map[string]*schema.Resource{"test_eds": r, "test_external_table": dependent}}
	}
	const address = testProviderAddress
	var stop context.CancelFunc
	var closed <-chan struct{}
	var reattach string
	start := func() {
		ctx, cancel := context.WithCancel(context.Background())
		config, ch, e := plugin.DebugServe(ctx, &plugin.ServeOpts{ProviderFunc: factory, ProviderAddr: address, Logger: hclog.NewNullLogger(), NoLogOutputOverride: true})
		if e != nil {
			cancel()
		}
		require.NoError(t, e)
		b, e := json.Marshal(map[string]plugin.ReattachConfig{address: config})
		require.NoError(t, e)
		reattach = string(b)
		stop = cancel
		closed = ch
	}
	start()
	t.Cleanup(func() {
		stop()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Error("provider server failed to close")
		}
	})
	dir := cli.dir
	plugins := filepath.Join(dir, "plugins")
	binary := testProviderBinary(t, plugins)
	// Init needs a package for its lockfile; TF_REATTACH_PROVIDERS runs the real
	// factory server above. The placeholder must never execute.
	require.NoError(t, os.WriteFile(binary, []byte("#!/bin/sh\nexit 91\n"), 0o600))
	require.NoError(t, os.Chmod(binary, 0o700))
	run := func(args ...string) (string, error) {
		cli.env = []string{"TF_REATTACH_PROVIDERS=" + reattach}
		out, e := cli.run(args...)
		require.False(t, strings.Contains(out, wireToken), "caller token leaked into Terraform output")
		return out, e
	}
	config := func(location, mode string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "main.tf"), []byte(fmt.Sprintf(`terraform {
 required_providers {
  test = { source = "example/test", version = "0.0.1" }
 }
}
resource "test_eds" "example" {
 connection_string = "grpc://%s/?database=/owner"
 path = "eds"
 source_type = "Ydb"
 location = "%s"
 auth_method = "IAM"
 service_account_id = "runtime-sa"
 database_id = "target"
 %s
 use_tls = false
}
resource "test_external_table" "dependent" {
 data_source = test_eds.example.id
}
`, f.addr, location, mode)), 0o600))
	}
	state := func() map[string]interface{} {
		raw, e := os.ReadFile(filepath.Join(dir, "terraform.tfstate"))
		require.NoError(t, e)
		var doc map[string]interface{}
		require.NoError(t, json.Unmarshal(raw, &doc))
		require.False(t, containsStringValue(doc, wireToken), "token leaked into persisted state")
		for _, v := range doc["resources"].([]interface{}) {
			resource := v.(map[string]interface{})
			if resource["type"] == "test_eds" {
				instance := resource["instances"].([]interface{})[0].(map[string]interface{})
				require.NotEqual(t, "tainted", instance["status"], "confirmed EDS must never be tainted by cleanup failure")
				return instance["attributes"].(map[string]interface{})
			}
		}
		t.Fatal("EDS missing from persisted state")
		return nil
	}
	counts := func() (creates, replaces, drops, secrets int) {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, q := range f.queries {
			switch {
			case strings.HasPrefix(q, "CREATE EXTERNAL DATA SOURCE"):
				creates++
			case strings.HasPrefix(q, "CREATE OR REPLACE EXTERNAL DATA SOURCE"):
				replaces++
			case strings.HasPrefix(q, "DROP EXTERNAL DATA SOURCE"):
				drops++
			case strings.Contains(q, "CREATE SECRET"):
				secrets++
			}
		}
		return
	}
	config("target:2135", `iam_bootstrap_mode = "CALLER_TOKEN"`)
	out, err := run("init", "-plugin-dir="+plugins, "-no-color")
	require.NoError(t, err, out)
	out, err = run("apply", "-auto-approve", "-no-color")
	require.NoError(t, err, out)
	require.Contains(t, out, "cleanup pending")
	attrs := state()
	require.NotEmpty(t, attrs["pending_cleanup_path"])
	id := attrs["id"].(string)
	require.EqualValues(t, 1, dependentCreates.Load())
	require.EqualValues(t, 0, dependentDeletes.Load())
	out, err = run("plan", "-no-color")
	require.Error(t, err)
	require.Contains(t, out, "cannot clean confirmed-owned bootstrap secret")
	require.Equal(t, id, state()["id"])
	stop()
	<-closed
	start() // a fresh provider instance must recover from persisted state alone
	f.mu.Lock()
	f.failDrop = false
	f.mu.Unlock()
	out, err = run("apply", "-auto-approve", "-no-color")
	require.NoError(t, err, out)
	require.Equal(t, "", state()["pending_cleanup_path"])
	out, err = run("plan", "-detailed-exitcode", "-no-color")
	require.NoError(t, err, out)
	c, r, d, s := counts()
	require.Equal(t, 1, c)
	require.Zero(t, r)
	require.Zero(t, d)
	require.Equal(t, 1, s)
	require.EqualValues(t, 1, dependentCreates.Load())
	require.EqualValues(t, 0, dependentDeletes.Load())
	out, err = run("show", "-json")
	require.NoError(t, err, out)
	out, err = run("plan", "-out=verified.tfplan", "-no-color")
	require.NoError(t, err, out)
	out, err = run("show", "-json", "verified.tfplan")
	require.NoError(t, err, out)
	config("other-target:2135", `iam_bootstrap_mode = "CALLER_TOKEN"`)
	out, err = run("apply", "-auto-approve", "-no-color")
	require.NoError(t, err, out)
	_, r, d, s = counts()
	require.Equal(t, 1, r)
	require.Zero(t, d)
	require.Equal(t, 2, s)
	out, err = run("state", "rm", "test_eds.example")
	require.NoError(t, err, out)
	config("other-target:2135", "")
	out, err = run("import", "-no-color", "test_eds.example", id)
	require.NoError(t, err, out)
	require.Equal(t, "EXISTING_SECRET", state()["iam_bootstrap_mode"])
	_, r, d, s = counts()
	require.Equal(t, 1, r)
	require.Zero(t, d)
	require.Equal(t, 2, s)
	// Read/import succeeds after the initial secret was dropped. A plan without
	// a configured EXISTING_SECRET path must fail clearly instead of adopting it.
	out, err = run("plan", "-no-color")
	require.Error(t, err)
	require.Contains(t, out, "must be configured")
	config("other-target:2135", `iam_bootstrap_mode = "CALLER_TOKEN"`)
	out, err = run("apply", "-auto-approve", "-no-color")
	require.NoError(t, err, out)
	out, err = run("plan", "-detailed-exitcode", "-no-color")
	require.NoError(t, err, out)
	out, err = run("destroy", "-auto-approve", "-no-color")
	require.NoError(t, err, out)
	require.EqualValues(t, 1, dependentDeletes.Load())
	f.mu.Lock()
	require.Empty(t, f.secrets)
	require.False(t, f.exists)
	require.True(t, f.parameterOK)
	f.mu.Unlock()
}

func containsStringValue(value interface{}, needle string) bool {
	switch v := value.(type) {
	case string:
		return strings.Contains(v, needle)
	case []interface{}:
		for _, item := range v {
			if containsStringValue(item, needle) {
				return true
			}
		}
	case map[string]interface{}:
		for key, item := range v {
			if strings.Contains(key, needle) || containsStringValue(item, needle) {
				return true
			}
		}
	}
	return false
}

func TestStateTokenDetectionHandlesJSONEscaping(t *testing.T) {
	raw, err := json.Marshal(map[string]interface{}{"resources": []interface{}{map[string]interface{}{"value": wireToken}}})
	require.NoError(t, err)
	require.NotContains(t, string(raw), wireToken)
	var state interface{}
	require.NoError(t, json.Unmarshal(raw, &state))
	require.True(t, containsStringValue(state, wireToken))
}
