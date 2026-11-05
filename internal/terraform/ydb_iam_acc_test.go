package terraform_test

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

// TestAccYdbExternalDataSource_iamCallerToken reads real target data after IAM
// bootstrap. Both databases, delegation and runtime grants must already exist.
func TestAccYdbExternalDataSource_iamCallerToken(t *testing.T) {
	if os.Getenv("TF_ACC") != "1" || os.Getenv("YDB_ACC_IAM_LIVE") != "1" {
		t.Skip("set TF_ACC=1 and YDB_ACC_IAM_LIVE=1 for cloud IAM acceptance")
	}
	for _, key := range []string{
		envAccYDBConnection, "YDB_ACC_TOKEN", "YDB_ACC_IAM_TARGET_CONNECTION_STRING",
		"YDB_ACC_IAM_SERVICE_ACCOUNT_ID", "YDB_ACC_IAM_FIXTURE_PREFIX",
	} {
		if os.Getenv(key) == "" {
			t.Fatalf("live IAM acceptance requires %s", key)
		}
	}
	prefix := os.Getenv("YDB_ACC_IAM_FIXTURE_PREFIX")
	if !regexp.MustCompile(`^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$`).MatchString(prefix) {
		t.Fatal("YDB_ACC_IAM_FIXTURE_PREFIX must be a relative namespace of letters, digits, underscores, hyphens and slashes")
	}
	ownerConn := os.Getenv(envAccYDBConnection)
	targetConn := os.Getenv("YDB_ACC_IAM_TARGET_CONNECTION_STRING")
	targetURL, err := url.Parse(targetConn)
	if err != nil || (targetURL.Scheme != "grpc" && targetURL.Scheme != "grpcs") ||
		targetURL.Host == "" || !strings.HasPrefix(targetURL.Query().Get("database"), "/") {
		t.Fatal("YDB_ACC_IAM_TARGET_CONNECTION_STRING must contain an endpoint and full database path")
	}
	location, databaseName, useTLS := targetURL.Host, targetURL.Query().Get("database"), targetURL.Scheme == "grpcs"
	fixturePath := prefix + "/fixture_" + accRandomHex8(t)
	edsPath := prefix + "/eds_" + accRandomHex8(t)

	// Keep credentials out of HCL while exercising the normal provider configure path.
	factories := accProviderFactories()
	newProvider := factories["ydb"]
	factories["ydb"] = func() (*schema.Provider, error) {
		p, err := newProvider()
		if err == nil {
			p.Schema["token"].DefaultFunc = schema.EnvDefaultFunc("YDB_ACC_TOKEN", nil)
		}
		return p, err
	}
	tableConfig := accConnectionStringVarBlock(ownerConn) + fmt.Sprintf(`
provider "ydb" {}

resource "ydb_table" "fixture" {
  connection_string = %q
  path              = %q
  column {
    name = "id"
    type = "Uint64"
  }
  primary_key = ["id"]
}
`, targetConn, fixturePath)
	iamConfig := tableConfig + fmt.Sprintf(`
resource "ydb_external_data_source" "iam" {
  depends_on = [ydb_table.fixture]

  connection_string  = var.connection_string
  path               = %q
  source_type        = "Ydb"
  location           = %q
  database_name      = %q
  auth_method        = "IAM"
  service_account_id = %q
  use_tls            = %t
}
`, edsPath, location, databaseName, os.Getenv("YDB_ACC_IAM_SERVICE_ACCOUNT_ID"), useTLS)
	seedTarget := func(_ *terraform.State) error {
		db, err := ydb.Open(t.Context(), targetConn, ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACC_TOKEN")))
		if err != nil {
			return fmt.Errorf("open target database: %w", err)
		}
		defer func() { _ = db.Close(t.Context()) }()
		return db.Query().Exec(t.Context(), fmt.Sprintf("UPSERT INTO `%s` (id) VALUES (42u)", helpers.EscapeYQLIdentifier(fixturePath)))
	}
	readThroughIAM := func(_ *terraform.State) error {
		db, err := ydb.Open(t.Context(), ownerConn, ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACC_TOKEN")))
		if err != nil {
			return fmt.Errorf("open owner database: %w", err)
		}
		defer func() { _ = db.Close(t.Context()) }()
		// YDB federated-query syntax: EDS identifier, then a table path relative to the target DB.
		query := fmt.Sprintf("SELECT id FROM `%s`.`%s` WHERE id = 42u",
			helpers.EscapeYQLIdentifier(edsPath), helpers.EscapeYQLIdentifier(fixturePath))
		row, err := db.Query().QueryRow(t.Context(), query)
		if err != nil {
			return fmt.Errorf("read target row through IAM EDS: %w", err)
		}
		var got uint64
		if err := row.Scan(&got); err != nil {
			return fmt.Errorf("scan target row through IAM EDS: %w", err)
		}
		if got != 42 {
			return fmt.Errorf("target row through IAM EDS: got %d, want 42", got)
		}
		return nil
	}
	resource.Test(t, resource.TestCase{
		ProviderFactories: factories,
		Steps: []resource.TestStep{
			{Config: tableConfig, Check: seedTarget},
			{Config: iamConfig, Check: readThroughIAM},
			{Config: iamConfig, PlanOnly: true},
			{ResourceName: "ydb_external_data_source.iam", ImportState: true, ImportStateVerify: true},
		},
	})
}
