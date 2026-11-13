package terraform_test

import (
	"fmt"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccYdbExternalDataSource_sharedReading(t *testing.T) {
	cli := newIAMTerraform(t)
	f := newIAMWireDB(t)
	const address = "ydb_external_data_source.test"
	config := func(sharedReading string) {
		cli.write("main.tf", fmt.Sprintf(`terraform {
  required_providers { ydb = { source = "terraform.storage.ydb.tech/provider/ydb" } }
}
provider "ydb" { token = "synthetic-terraform-caller-token" }
resource "ydb_external_data_source" "test" {
  connection_string = "grpc://%s/?database=/owner"
  path = "eds"
  source_type = "Ydb"
  location = "target:2135"
  database_name = "/target"
  auth_method = "IAM"
  service_account_id = "runtime-sa"
  %s
}
data "ydb_external_data_source" "test" {
  connection_string = ydb_external_data_source.test.connection_string
  path = ydb_external_data_source.test.path
}
`, f.addr, sharedReading))
	}
	run := func(args ...string) {
		t.Helper()
		out, err := cli.run(args...)
		require.NoError(t, err, "%s", out)
	}
	var id interface{}
	for _, step := range []struct {
		name     string
		argument string
		property string
		want     bool
	}{
		{name: "enable", argument: "shared_reading = true", property: "TRUE", want: true},
		{name: "disable", argument: "shared_reading = false", property: "FALSE"},
		{name: "enable again", argument: "shared_reading = true", property: "TRUE", want: true},
		{name: "remove argument"},
	} {
		t.Log(step.name)
		config(step.argument)
		run("apply", "-auto-approve", "-no-color")
		state := cli.state(address)
		if id == nil {
			id = state["id"]
			require.NotEmpty(t, id)
		}
		require.Equal(t, id, state["id"], "changing shared reading must update the EDS in place")
		require.Equal(t, step.want, state["shared_reading"])
		require.Equal(t, step.want, cli.state("data." + address)["shared_reading"])
		f.mu.Lock()
		property := f.properties["SHARED_READING"]
		f.mu.Unlock()
		require.Equal(t, step.property, property)
		run("plan", "-detailed-exitcode", "-no-color")
	}
	config("shared_reading = true")
	run("apply", "-auto-approve", "-no-color")
	run("state", "rm", address)
	run("import", "-no-color", address, id.(string))
	require.Equal(t, true, cli.state(address)["shared_reading"])
	run("plan", "-detailed-exitcode", "-no-color")

	f.mu.Lock()
	delete(f.properties, "SHARED_READING")
	f.mu.Unlock()
	out, err := cli.run("plan", "-detailed-exitcode", "-no-color")
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "%s", out)
	require.Equal(t, 2, exitErr.ExitCode(), "%s", out)
	require.Contains(t, out, "shared_reading")
	run("apply", "-auto-approve", "-no-color")
	require.Equal(t, id, cli.state(address)["id"])
	require.Equal(t, true, cli.state(address)["shared_reading"])
	require.Equal(t, true, cli.state("data." + address)["shared_reading"])
	f.mu.Lock()
	property := f.properties["SHARED_READING"]
	f.mu.Unlock()
	require.Equal(t, "TRUE", property)
	run("plan", "-detailed-exitcode", "-no-color")
	run("destroy", "-auto-approve", "-no-color")
}

func TestAccYdbExternalDataSource_withoutSharedReading(t *testing.T) {
	cli := newIAMTerraform(t)
	f := newIAMWireDB(t)
	const address = "ydb_external_data_source.test"
	var id interface{}
	for _, location := range []string{"https://example.test/bucket", "https://example.test/updated-bucket"} {
		cli.write("main.tf", fmt.Sprintf(`terraform {
  required_providers { ydb = { source = "terraform.storage.ydb.tech/provider/ydb" } }
}
provider "ydb" {}
resource "ydb_external_data_source" "test" {
  connection_string = "grpc://%s/?database=/owner"
  path = "eds"
  source_type = "ObjectStorage"
  location = %q
  auth_method = "NONE"
}
`, f.addr, location))
		out, err := cli.run("apply", "-auto-approve", "-no-color")
		require.NoError(t, err, "%s", out)
		state := cli.state(address)
		if id == nil {
			id = state["id"]
			require.NotEmpty(t, id)
		}
		require.Equal(t, id, state["id"])
		require.Equal(t, location, state["location"])
		f.mu.Lock()
		_, hasSharedReading := f.properties["SHARED_READING"]
		storedLocation := f.properties["LOCATION"]
		f.mu.Unlock()
		require.False(t, hasSharedReading, "an omitted option must stay absent after refresh and unrelated updates")
		require.Equal(t, location, storedLocation)
		out, err = cli.run("plan", "-detailed-exitcode", "-no-color")
		require.NoError(t, err, "%s", out)
	}
	out, err := cli.run("destroy", "-auto-approve", "-no-color")
	require.NoError(t, err, "%s", out)
}
