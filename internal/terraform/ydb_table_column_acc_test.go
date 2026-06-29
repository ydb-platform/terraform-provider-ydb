package terraform_test

import (
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

// TestAccYdbTable_columnTypeChangePlanError verifies that changing a column type fails at plan time.
func TestAccYdbTable_columnTypeChangePlanError(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	tblPath := "tf_acc_col_type/tbl_" + suffix

	configInitial := accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_table" "test" {
  connection_string = var.connection_string
  path              = %q

  column {
    name = "pk"
    type = "Utf8"
  }
  column {
    name = "val"
    type = "Uint32"
  }

  primary_key = ["pk"]
}
`, tblPath)

	configTypeChanged := accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_table" "test" {
  connection_string = var.connection_string
  path              = %q

  column {
    name = "pk"
    type = "Utf8"
  }
  column {
    name = "val"
    type = "Uint64"
  }

  primary_key = ["pk"]
}
`, tblPath)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: configInitial,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_table.test", "path", tblPath),
					resource.TestCheckResourceAttrSet("ydb_table.test", "id"),
					resource.TestCheckTypeSetElemNestedAttrs("ydb_table.test", "column.*", map[string]string{
						"name": "val",
						"type": "Uint32",
					}),
				),
			},
			{
				Config:      configTypeChanged,
				PlanOnly:    true,
				ExpectError: regexp.MustCompile(`changing column "val" type from "Uint32" to "Uint64" is not supported`),
			},
		},
	})
}

// TestAccYdbTable_columnOrderNoDrift verifies that column block order in HCL does not cause plan drift.
func TestAccYdbTable_columnOrderNoDrift(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	tblPath := "tf_acc_col_order/tbl_" + suffix

	configPKFirst := accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_table" "test" {
  connection_string = var.connection_string
  path              = %q

  column {
    name = "pk"
    type = "Utf8"
  }
  column {
    name = "val"
    type = "Uint32"
  }

  primary_key = ["pk"]
}
`, tblPath)

	configValFirst := accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_table" "test" {
  connection_string = var.connection_string
  path              = %q

  column {
    name = "val"
    type = "Uint32"
  }
  column {
    name = "pk"
    type = "Utf8"
  }

  primary_key = ["pk"]
}
`, tblPath)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: configPKFirst,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_table.test", "path", tblPath),
				),
			},
			{
				Config:   configValFirst,
				PlanOnly: true,
			},
		},
	})
}
