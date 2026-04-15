package terraform_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

// TestAccYdbExternalDataSource_ydbTokenWithSecret creates a secret, then a Ydb external data source
// with AUTH_METHOD = TOKEN. token_secret_path uses ydb_secret.path (full catalog path under the DB).
func TestAccYdbExternalDataSource_ydbTokenWithSecret(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	secretRel := "tf_acc_ext/tok_" + suffix
	dsPath := "tf_acc_ext/ds_tok_" + suffix
	loc := accLocationHostPortFromConn(conn)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_secret" "for_token" {
  connection_string = var.connection_string
  name                = %q
  value               = "acc-dummy-token-for-external-ds"
}

resource "ydb_external_data_source" "with_secret" {
  depends_on = [ydb_secret.for_token]

  connection_string = var.connection_string
  path                = %q
  source_type         = "Ydb"
  location            = %q
  auth_method         = "TOKEN"
  token_secret_path   = ydb_secret.for_token.path
}
`, secretRel, dsPath, loc),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_secret.for_token", "name", secretRel),
					resource.TestCheckResourceAttr("ydb_external_data_source.with_secret", "path", dsPath),
					resource.TestCheckResourceAttr("ydb_external_data_source.with_secret", "source_type", "Ydb"),
					resource.TestCheckResourceAttr("ydb_external_data_source.with_secret", "auth_method", "TOKEN"),
					resource.TestCheckResourceAttr("ydb_external_data_source.with_secret", "location", loc),
					resource.TestCheckResourceAttrPair("ydb_external_data_source.with_secret", "token_secret_path", "ydb_secret.for_token", "path"),
					resource.TestCheckResourceAttrSet("ydb_external_data_source.with_secret", "id"),
				),
			},
		},
	})
}

func TestAccYdbExternalDataSource_objectStorageNone(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	path := "tf_acc_ext/ds_" + suffix

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_external_data_source" "test" {
  connection_string = var.connection_string
  path                = %q
  source_type         = "ObjectStorage"
  location            = "https://example.com/terraform-acc-bucket/"
  auth_method         = "NONE"
}
`, path),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "path", path),
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "source_type", "ObjectStorage"),
					resource.TestCheckResourceAttrSet("ydb_external_data_source.test", "id"),
				),
			},
		},
	})
}

func TestAccYdbExternalTable_withDataSource(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	dsPath := "tf_acc_ext/ds_" + suffix
	tblPath := "tf_acc_ext/tbl_" + suffix

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_external_data_source" "s3" {
  connection_string = var.connection_string
  path                = %q
  source_type         = "ObjectStorage"
  location            = "https://example.com/terraform-acc-bucket/"
  auth_method         = "NONE"
}

resource "ydb_external_table" "test" {
  connection_string  = var.connection_string
  path                 = %q
  data_source_path     = ydb_external_data_source.s3.path
  location             = "prefix/"
  format               = "csv_with_names"

  column {
    name = "key"
    type = "Utf8"
  }
  column {
    name = "value"
    type = "Utf8"
  }
}
`, dsPath, tblPath),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_external_table.test", "path", tblPath),
					resource.TestCheckResourceAttrPair("ydb_external_table.test", "data_source_path", "ydb_external_data_source.s3", "path"),
					resource.TestCheckResourceAttr("ydb_external_table.test", "format", "csv_with_names"),
					resource.TestCheckResourceAttrSet("ydb_external_table.test", "id"),
				),
			},
		},
	})
}

func TestAccYdbExternalTable_dataSource(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	dsPath := "tf_acc_ext/ds_" + suffix
	tblPath := "tf_acc_ext/tbl_" + suffix

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_external_data_source" "s3" {
  connection_string = var.connection_string
  path                = %q
  source_type         = "ObjectStorage"
  location            = "https://example.com/terraform-acc-bucket/"
  auth_method         = "NONE"
}

resource "ydb_external_table" "src" {
  connection_string  = var.connection_string
  path                 = %q
  data_source_path     = ydb_external_data_source.s3.path
  location             = "data/"
  format               = "parquet"

  column {
    name     = "id"
    type     = "Int64"
    not_null = true
  }
}

data "ydb_external_table" "out" {
  connection_string = ydb_external_table.src.connection_string
  path              = ydb_external_table.src.path
}
`, dsPath, tblPath),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttrPair("data.ydb_external_table.out", "path", "ydb_external_table.src", "path"),
					resource.TestCheckResourceAttrPair("data.ydb_external_table.out", "format", "ydb_external_table.src", "format"),
					resource.TestCheckResourceAttrPair("data.ydb_external_table.out", "data_source_path", "ydb_external_table.src", "data_source_path"),
				),
			},
		},
	})
}

func TestAccYdbExternalDataSource_dataSource(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	path := "tf_acc_ext/ds_" + suffix

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_external_data_source" "src" {
  connection_string = var.connection_string
  path                = %q
  source_type         = "ObjectStorage"
  location            = "https://example.com/terraform-acc-ds-read/"
  auth_method         = "NONE"
}

data "ydb_external_data_source" "out" {
  connection_string = ydb_external_data_source.src.connection_string
  path              = ydb_external_data_source.src.path
}
`, path),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttrPair("data.ydb_external_data_source.out", "path", "ydb_external_data_source.src", "path"),
					resource.TestCheckResourceAttrPair("data.ydb_external_data_source.out", "source_type", "ydb_external_data_source.src", "source_type"),
					resource.TestCheckResourceAttrPair("data.ydb_external_data_source.out", "location", "ydb_external_data_source.src", "location"),
				),
			},
		},
	})
}
