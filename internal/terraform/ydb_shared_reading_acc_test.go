package terraform_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccYdbExternalDataSource_sharedReading(t *testing.T) {
	f := newIAMWireDB(t)
	const address = "ydb_external_data_source.test"
	conn := fmt.Sprintf("grpc://%s/?database=/owner", f.addr)
	config := func(argument string) string {
		return accConnectionStringVarBlock(conn) + fmt.Sprintf(`
provider "ydb" { token = "synthetic-terraform-caller-token" }
resource "ydb_external_data_source" "test" {
  connection_string = var.connection_string
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
`, argument)
	}
	var id string
	check := func(want bool) resource.TestCheckFunc {
		return resource.ComposeAggregateTestCheckFunc(
			accCheckSharedReading(f, &id, want),
			resource.TestCheckResourceAttr(address, "auth_method", "IAM"),
			resource.TestCheckResourceAttrPair("data."+address, "shared_reading", address, "shared_reading"),
		)
	}
	enabled := config("shared_reading = true")
	resource.Test(t, resource.TestCase{
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{Config: enabled, Check: check(true)},
			{Config: config("shared_reading = false"), Check: check(false)},
			{Config: enabled, Check: check(true)},
			{Config: config(""), Check: check(false)},
			{Config: enabled, Check: check(true)},
			{ResourceName: address, ImportState: true, ImportStateVerify: true},
			{
				PreConfig: func() {
					f.mu.Lock()
					delete(f.properties, "SHARED_READING")
					f.mu.Unlock()
				},
				Config: enabled,
				Check:  check(true),
			},
			{Config: enabled, PlanOnly: true},
		},
	})
}

func TestAccYdbExternalDataSource_withoutSharedReading(t *testing.T) {
	f := newIAMWireDB(t)
	const address = "ydb_external_data_source.test"
	conn := fmt.Sprintf("grpc://%s/?database=/owner", f.addr)
	config := func(location string) string {
		return accConnectionStringVarBlock(conn) + fmt.Sprintf(`
provider "ydb" {}
resource "ydb_external_data_source" "test" {
  connection_string = var.connection_string
  path = "eds"
  source_type = "ObjectStorage"
  location = %q
  auth_method = "NONE"
}
`, location)
	}
	var id string
	check := func(location string) resource.TestCheckFunc {
		return resource.ComposeAggregateTestCheckFunc(
			accCheckSharedReading(f, &id, false),
			resource.TestCheckResourceAttr(address, "location", location),
		)
	}
	const (
		initialLocation = "https://example.test/bucket"
		updatedLocation = "https://example.test/updated-bucket"
	)
	resource.Test(t, resource.TestCase{
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{Config: config(initialLocation), Check: check(initialLocation)},
			{Config: config(updatedLocation), Check: check(updatedLocation)},
			{Config: config(updatedLocation), PlanOnly: true},
		},
	})
}

func accCheckSharedReading(f *iamWireDB, id *string, want bool) resource.TestCheckFunc {
	const address = "ydb_external_data_source.test"
	return resource.ComposeAggregateTestCheckFunc(
		resource.TestCheckResourceAttr(address, "shared_reading", strconv.FormatBool(want)),
		resource.TestCheckResourceAttrSet(address, "id"),
		func(s *terraform.State) error {
			r, ok := s.RootModule().Resources[address]
			if !ok || r.Primary == nil {
				return fmt.Errorf("EDS %q is missing from state", address)
			}
			if *id != "" && r.Primary.ID != *id {
				return fmt.Errorf("EDS identity changed: got %q, want %q", r.Primary.ID, *id)
			}
			*id = r.Primary.ID
			f.mu.Lock()
			defer f.mu.Unlock()
			value, exists := f.properties["SHARED_READING"]
			if want && value != "TRUE" {
				return fmt.Errorf("EDS SHARED_READING: got %q, want TRUE", value)
			}
			if !want && exists {
				return fmt.Errorf("disabled EDS shared reading must omit SHARED_READING, got %q", value)
			}
			return nil
		},
	)
}
