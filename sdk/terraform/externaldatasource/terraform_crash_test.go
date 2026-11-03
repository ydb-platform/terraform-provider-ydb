package externaldatasource

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A process kill cannot checkpoint the secret path into Terraform state. This
// demonstrates that limit and proves rerun never sweeps or adopts by prefix.
func TestLocalTerraformYDBIAMCrashOrphan(t *testing.T) {
	cli := newTerraformCLI(t)
	f := newWireDB(t)
	f.blockMutation = true
	f.mutationEntered = make(chan struct{})
	dir := cli.dir
	plugins := filepath.Join(dir, "plugins")
	buildTestProvider(t, testProviderBinary(t, plugins), "./testdata/error-provider")
	pidfile := filepath.Join(dir, "provider.pid")
	cli.env = []string{"YDB_TEST_REAL_PROVIDER=1", "YDB_TEST_PID_FILE=" + pidfile}
	run := func(args ...string) (string, error) {
		out, err := cli.run(args...)
		require.False(t, strings.Contains(out, wireToken), "token in process output")
		return out, err
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "main.tf"), []byte(fmt.Sprintf(`terraform {
 required_providers {
  test = { source = "example/test", version = "0.0.1" }
 }
}
resource "test_eds" "example" {
 connection_string = "grpc://%s/?database=/owner"
 path = "eds"
 source_type = "Ydb"
 location = "target:2135"
 database_id = "target"
 auth_method = "IAM"
 service_account_id = "runtime-sa"
}
`, f.addr)), 0o600))
	result, err := run("init", "-plugin-dir="+plugins, "-no-color")
	require.NoError(t, err, result)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	apply := cli.command(ctx, "apply", "-auto-approve", "-no-color")
	finished := make(chan error, 1)
	var output []byte
	go func() { var e error; output, e = apply.CombinedOutput(); finished <- e }()
	select {
	case <-f.mutationEntered:
	case <-ctx.Done():
		t.Fatal("provider did not reach EDS mutation")
	}
	raw, err := os.ReadFile(pidfile)
	require.NoError(t, err)
	pid, err := strconv.Atoi(string(raw))
	require.NoError(t, err)
	process, err := os.FindProcess(pid)
	require.NoError(t, err)
	require.NoError(t, process.Kill())
	select {
	case err = <-finished:
		require.Error(t, err)
	case <-ctx.Done():
		_ = apply.Process.Kill()
		t.Fatal("Terraform did not report killed provider")
	}
	require.False(t, strings.Contains(string(output), wireToken), "token in killed-provider output")
	f.mu.Lock()
	require.Len(t, f.secrets, 1)
	var orphan string
	for p := range f.secrets {
		orphan = p
	}
	require.False(t, f.exists)
	f.mu.Unlock()
	result, err = run("state", "list")
	require.NoError(t, err, result)
	require.NotContains(t, result, "test_eds.example")
	result, err = run("apply", "-auto-approve", "-no-color")
	require.NoError(t, err, result)
	f.mu.Lock()
	require.Len(t, f.secrets, 1)
	require.True(t, f.secrets[orphan], "rerun must retain orphan from uncheckpointed attempt")
	require.True(t, f.exists)
	f.mu.Unlock()
	result, err = run("destroy", "-auto-approve", "-no-color")
	require.NoError(t, err, result)
	f.mu.Lock()
	require.Len(t, f.secrets, 1)
	require.True(t, f.secrets[orphan])
	f.mu.Unlock()
	t.Log("killed provider left an uncheckpointed orphan; rerun used a new path without sweeping the old secret")
}
