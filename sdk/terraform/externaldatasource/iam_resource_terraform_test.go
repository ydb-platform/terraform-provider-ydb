package externaldatasource

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIAMTerraformRejectsEmptyResourceID(t *testing.T) {
	cli := newIAMTerraform(t)
	for _, resourceID := range []string{"", "  "} {
		t.Run(fmt.Sprintf("resource_id=%q", resourceID), func(t *testing.T) {
			cli.write("main.tf", fmt.Sprintf(`terraform {
  required_providers { ydb = { source = %q } }
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
  resource_id = %q
}
`, iamTestProviderAddress, resourceID))
			out, err := cli.run("plan", "-no-color")
			require.Error(t, err)
			require.Contains(t, out, "resource_id")
			require.Contains(t, out, "empty string or whitespace")
		})
	}
}
