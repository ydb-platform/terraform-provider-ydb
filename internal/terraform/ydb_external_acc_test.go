package terraform_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"

	providerimpl "github.com/ydb-platform/terraform-provider-ydb/internal/terraform"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"

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
  database_name       = "/local"
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
					resource.TestCheckResourceAttr("ydb_external_data_source.with_secret", "database_name", "/local"),
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
  database_name       = "/local"
}
`, dsPath, ydbLoc),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "source_type", "Ydb"),
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "location", ydbLoc),
					resource.TestCheckResourceAttr("ydb_external_data_source.test", "database_name", "/local"),
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

// This live gate requires pre-existing delegation/grants and two distinct cloud
// identities. It does not establish runtime-query/restart/version compatibility.
func TestAccYDBExternalDataSourceIAM(t *testing.T) {
	if os.Getenv("TF_ACC") != "1" || os.Getenv("YDB_ACC_IAM_LIVE") != "1" {
		t.Skip("set TF_ACC=1 and YDB_ACC_IAM_LIVE=1 for dedicated cloud IAM DDL acceptance")
	}
	for _, key := range []string{envAccYDBConnection, "YDB_ACC_TOKEN", "YDB_ACC_IAM_TARGET_DATABASE_ID", "YDB_ACC_IAM_SERVICE_ACCOUNT_ID", "YDB_ACC_IAM_TARGET_LOCATION", "YDB_ACC_IAM_TARGET_CLOUD_ID"} {
		if os.Getenv(key) == "" {
			t.Skipf("live IAM gate requires %s", key)
		}
	}
	conn := os.Getenv(envAccYDBConnection)
	dsPath := "tf_acc_iam_" + accRandomHex8(t)
	// Supply transport credentials in the provider factory, never in test HCL/plan.
	factories := accProviderFactories()
	original := factories["ydb"]
	factories["ydb"] = func() (*schema.Provider, error) {
		p, err := original()
		if err != nil {
			return nil, err
		}
		p.ConfigureContextFunc = func(context.Context, *schema.ResourceData) (interface{}, diag.Diagnostics) {
			return &providerimpl.Config{AuthCreds: auth.YdbCredentials{Token: os.Getenv("YDB_ACC_TOKEN")}}, nil
		}
		return p, nil
	}
	config := accConnectionStringVarBlock(conn) + fmt.Sprintf(`
provider "ydb" {}
resource "ydb_external_data_source" "iam" {
 connection_string = var.connection_string
 path = %q
 source_type = "Ydb"
 location = %q
 database_id = %q
 auth_method = "IAM"
 service_account_id = %q
 iam_bootstrap_mode = "CALLER_TOKEN"
 use_tls = true
}
`, dsPath, os.Getenv("YDB_ACC_IAM_TARGET_LOCATION"), os.Getenv("YDB_ACC_IAM_TARGET_DATABASE_ID"), os.Getenv("YDB_ACC_IAM_SERVICE_ACCOUNT_ID"))
	check := func(state *terraform.State) error {
		attrs := state.RootModule().Resources["ydb_external_data_source.iam"].Primary.Attributes
		db := accOpenYDB(t)
		p := attrs["initial_token_secret_path"]
		if p == "" {
			return fmt.Errorf("missing observed bootstrap path")
		}
		dir, err := db.Scheme().ListDirectory(t.Context(), path.Dir(p))
		if err != nil {
			return fmt.Errorf("cannot verify temporary secret absence")
		}
		for _, entry := range dir.Children {
			if entry.Name == path.Base(p) {
				return fmt.Errorf("temporary secret was not removed")
			}
		}
		return nil
	}
	resource.Test(t, resource.TestCase{ProviderFactories: factories, Steps: []resource.TestStep{
		{Config: config, Check: resource.ComposeAggregateTestCheckFunc(resource.TestCheckResourceAttr("ydb_external_data_source.iam", "resource_id", os.Getenv("YDB_ACC_IAM_TARGET_CLOUD_ID")), resource.TestCheckResourceAttr("ydb_external_data_source.iam", "pending_cleanup_path", ""), check)},
		{Config: config, PlanOnly: true},
		{ResourceName: "ydb_external_data_source.iam", ImportState: true, ImportStateVerify: true, ImportStateVerifyIgnore: []string{"iam_bootstrap_mode"}},
	}})
}
