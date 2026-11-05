package terraform_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccYdbExternalDataSource_iamRejectsEmptyResourceID(t *testing.T) {
	cli := newIAMTerraform(t)
	for _, tc := range []struct {
		name, argument string
	}{
		{name: "empty", argument: `resource_id = ""`},
		{name: "whitespace", argument: `resource_id = "  "`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli.write("main.tf", strings.ReplaceAll(`terraform {
  required_providers { ydb = { source = "terraform.storage.ydb.tech/provider/ydb" } }
}
provider "ydb" { token = "synthetic-caller" }
resource "ydb_external_data_source" "test" {
  connection_string = "grpc://localhost:2136/?database=/owner"
  path = "eds"
  source_type = "Ydb"
  location = "target:2135"
  database_name = "/target"
  auth_method = "IAM"
  service_account_id = "runtime-sa"
  resource_id = ""
}
`, `resource_id = ""`, tc.argument))
			out, err := cli.run("plan", "-no-color")
			require.Error(t, err)
			require.Contains(t, out, "resource_id")
			require.Contains(t, out, "empty string or whitespace")
		})
	}
}
