package terraform_test

import (
	"fmt"
	"maps"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/stretchr/testify/require"
)

func TestAccYdbExternalDataSource_iamCatalog(t *testing.T) {
	if os.Getenv("TF_ACC") != "1" {
		t.Skip("set TF_ACC=1 to run acceptance tests")
	}
	if os.Getenv("YDB_ACC_IAM_ACCESS_SERVICE_ADDR") == "" {
		t.Skip("local IAM catalog fixture requires YDB_ACC_IAM_ACCESS_SERVICE_ADDR")
	}
	accPreCheckYDB(t)
	conn := os.Getenv(envAccYDBConnection)
	u, err := url.Parse(conn)
	require.NoError(t, err)
	root := u.Query().Get("database")
	require.NotEmpty(t, root)
	db := accOpenYDB(t)
	if !accIAMSecretParametersSupported(t, db, path.Join(root, "__tf_acc_iam_probe_"+accRandomHex8(t))) {
		t.Skip("this YDB server rejects Utf8 parameters in CREATE SECRET; safe rejection is covered by iamCreatePreservesExisting")
	}
	before, err := accIAMSecretPaths(t.Context(), db, root)
	require.NoError(t, err)
	name := "tf_acc_iam/source_" + accRandomHex8(t)
	fullPath := path.Join(root, name)
	config := accConnectionStringVarBlock(conn) + strings.NewReplacer(
		`"source-path"`, strconv.Quote(name), `"target:2136"`, strconv.Quote(accLocationHostPortFromConn(conn)), `"/target"`, strconv.Quote(root),
	).Replace(`provider "ydb" { token = "synthetic-local-iam-caller" }
resource "ydb_external_data_source" "iam" {
  connection_string = var.connection_string
  path = "source-path"
  source_type = "Ydb"
  location = "target:2136"
  database_name = "/target"
  auth_method = "IAM"
  service_account_id = "runtime-sa"
  use_tls = false
}
`)
	updated := strings.Replace(config, `"runtime-sa"`, `"updated-runtime-sa"`, 1)
	var id string
	check := func(sa string) resource.TestCheckFunc {
		return resource.ComposeAggregateTestCheckFunc(
			resource.TestCheckResourceAttr("ydb_external_data_source.iam", "auth_method", "IAM"),
			resource.TestCheckResourceAttr("ydb_external_data_source.iam", "service_account_id", sa),
			func(s *terraform.State) error {
				current := s.RootModule().Resources["ydb_external_data_source.iam"].Primary.ID
				if id != "" && current != id {
					return fmt.Errorf("EDS identity changed: got %q, want %q", current, id)
				}
				id = current
				desc, err := db.Table().DescribeExternalDataSource(t.Context(), fullPath)
				if err != nil {
					return err
				}
				if desc.Properties["AUTH_METHOD"] != "IAM" || desc.Properties["SERVICE_ACCOUNT_ID"] != sa {
					return fmt.Errorf("unexpected EDS authentication: %v", desc.Properties)
				}
				after, err := accIAMSecretPaths(t.Context(), db, root)
				if err == nil && !maps.Equal(before, after) {
					return fmt.Errorf("temporary IAM secrets changed: before %v, after %v", before, after)
				}
				return err
			},
		)
	}
	resource.Test(t, resource.TestCase{
		ProviderFactories: accProviderFactories(),
		CheckDestroy: func(_ *terraform.State) error {
			dir, err := db.Scheme().ListDirectory(t.Context(), path.Dir(fullPath))
			if err != nil {
				return err
			}
			for _, child := range dir.Children {
				if child.Name == path.Base(fullPath) {
					return fmt.Errorf("EDS %q still exists after destroy", fullPath)
				}
			}
			return nil
		},
		Steps: []resource.TestStep{
			{Config: config, Check: check("runtime-sa")},
			{Config: config, PlanOnly: true},
			{Config: updated, Check: check("updated-runtime-sa")},
			{ResourceName: "ydb_external_data_source.iam", ImportState: true, ImportStateVerify: true},
			{Config: updated, PlanOnly: true},
		},
	})
}
