package terraform_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

// TestAccYdbTable_ttlBlockReadAfterApply creates a table with a ttl block via HCL,
// which runs Create then Read. Read must not panic: ttl is a TypeSet in the schema
// and flattenTableDescription must d.Set a *schema.Set, not a plain map, or Terraform
// SDK would panic in MapFieldWriter.setSet.
func TestAccYdbTable_ttlBlockReadAfterApply(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	tblPath := "tf_acc_ttl/tbl_" + suffix

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_table" "test" {
  connection_string = var.connection_string
  path              = %q

  column {
    name = "pk"
    type = "Utf8"
  }
  column {
    name     = "ttl_col"
    type     = "Uint32"
  }

  primary_key = ["pk"]

  ttl {
    column_name     = "ttl_col"
    expire_interval  = "PT1H"
    unit             = "seconds"
  }
}
`, tblPath),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_table.test", "path", tblPath),
					resource.TestCheckResourceAttrSet("ydb_table.test", "id"),
					resource.TestCheckTypeSetElemNestedAttrs("ydb_table.test", "ttl.*", map[string]string{
						"column_name":     "ttl_col",
						"expire_interval": "PT1H",
						"unit":            "seconds",
					}),
				),
			},
		},
	})
}
