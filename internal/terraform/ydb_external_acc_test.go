package terraform_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

// TestAccYdbExternalDataSource_ydbTokenWithSecret creates a secret, then a Ydb external data source
// with AUTH_METHOD = TOKEN. token_secret_path uses ydb_secret.path (full catalog path under the DB).
// The second step re-plans the same config to assert no drift — flattenDescription must round-trip
// every populated *_SECRET_PATH attribute. ExpectNonEmptyPlan defaults to false, so any planned
// change fails the test.
func TestAccYdbExternalDataSource_ydbTokenWithSecret(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	secretRel := "tf_acc_ext/tok_" + suffix
	dsPath := "tf_acc_ext/ds_tok_" + suffix
	loc := accLocationHostPortFromConn(conn)

	config := accTestConfigPrefix(conn) + fmt.Sprintf(`
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
`, secretRel, dsPath, loc)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: config,
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
			{
				Config:   config,
				PlanOnly: true,
			},
		},
	})
}

// TestAccYdbExternalDataSource_updateInPlaceWithDependentTable verifies that EDS
// configuration is updated via CREATE OR REPLACE while an external table references it.
// A destroy-and-create update (ForceNew) would fail: YDB rejects dropping an external
// data source that has dependent external tables.
func TestAccYdbExternalDataSource_updateInPlaceWithDependentTable(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	dsPath := "tf_acc_ext/ds_" + suffix
	tblPath := "tf_acc_ext/tbl_" + suffix
	locInitial := "https://example.com/terraform-acc-bucket/"
	locUpdated := "https://example.com/terraform-acc-bucket-updated/"

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
  location            = %q
  auth_method         = "NONE"
}

resource "ydb_external_table" "dependent" {
  connection_string  = var.connection_string
  path                 = %q
  data_source_path     = ydb_external_data_source.test.path
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
`, dsPath, locInitial, tblPath),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "path", dsPath),
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "source_type", "ObjectStorage"),
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "location", locInitial),
					resource.TestCheckResourceAttrSet("ydb_external_data_source.test", "id"),
					resource.TestCheckResourceAttr("ydb_external_table.dependent", "path", tblPath),
					resource.TestCheckResourceAttrPair("ydb_external_table.dependent", "data_source_path", "ydb_external_data_source.test", "path"),
					resource.TestCheckResourceAttrSet("ydb_external_table.dependent", "id"),
				),
			},
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_external_data_source" "test" {
  connection_string = var.connection_string
  path                = %q
  source_type         = "ObjectStorage"
  location            = %q
  auth_method         = "NONE"
}

resource "ydb_external_table" "dependent" {
  connection_string  = var.connection_string
  path                 = %q
  data_source_path     = ydb_external_data_source.test.path
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
`, dsPath, locUpdated, tblPath),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "location", locUpdated),
					resource.TestCheckResourceAttr("ydb_external_table.dependent", "path", tblPath),
					resource.TestCheckResourceAttrPair("ydb_external_table.dependent", "data_source_path", "ydb_external_data_source.test", "path"),
				),
			},
		},
	})
}

// TestAccYdbExternalDataSource_sourceTypeRequiresReplacement verifies that changing
// source_type is applied via ForceNew (destroy + create), not an in-place update.
func TestAccYdbExternalDataSource_sourceTypeRequiresReplacement(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	dsPath := "tf_acc_ext/ds_st_" + suffix
	ydbLoc := accLocationHostPortFromConn(conn)

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
`, dsPath),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "source_type", "ObjectStorage"),
				),
			},
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_external_data_source" "test" {
  connection_string = var.connection_string
  path                = %q
  source_type         = "Ydb"
  location            = %q
  auth_method         = "NONE"
}
`, dsPath, ydbLoc),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "source_type", "Ydb"),
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "location", ydbLoc),
				),
			},
		},
	})
}

// TestAccYdbExternalTable_withDataSource creates an external table backed by an EDS, then
// re-plans the same config. Column types are lowercase in HCL; YDB Describe returns
// PascalCase — Read must normalize so the second plan is empty.
func TestAccYdbExternalTable_withDataSource(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	dsPath := "tf_acc_ext/ds_" + suffix
	tblPath := "tf_acc_ext/tbl_" + suffix

	config := accTestConfigPrefix(conn) + fmt.Sprintf(`
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
    name     = "id"
    type     = "int32"
    not_null = true
  }
  column {
    name = "name"
    type = "string"
  }
  column {
    name     = "key"
    type     = "utf8"
    not_null = true
  }
  column {
    name = "value"
    type = "Utf8"
  }
}
`, dsPath, tblPath)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_external_table.test", "path", tblPath),
					resource.TestCheckResourceAttrPair("ydb_external_table.test", "data_source_path", "ydb_external_data_source.s3", "path"),
					resource.TestCheckResourceAttr("ydb_external_table.test", "format", "csv_with_names"),
					resource.TestCheckResourceAttrSet("ydb_external_table.test", "id"),
				),
			},
			{
				Config:   config,
				PlanOnly: true,
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
