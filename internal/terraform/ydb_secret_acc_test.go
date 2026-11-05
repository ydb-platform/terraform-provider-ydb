package terraform_test

import (
	"os"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

// Acceptance tests for ydb_secret (see acc_test.go for env and how to run).

func TestAccYdbSecret_basic(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	name := "tf_acc_" + accRandomHex8(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + strings.ReplaceAll(`
resource "ydb_secret" "test" {
  connection_string   = var.connection_string
  name                  = "test-secret"
  value                 = "apostrophe' and double\" ending\\"
  inherit_permissions   = false
}
`, "test-secret", name),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_secret.test", "name", name),
					resource.TestCheckResourceAttrSet("ydb_secret.test", "id"),
					resource.TestCheckResourceAttrSet("ydb_secret.test", "path"),
				),
			},
		},
	})
}

func TestAccYdbSecret_inheritPermissions(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	name := "tf_acc_" + accRandomHex8(t)

	// inherit_permissions affects CREATE SECRET only; YDB Describe does not return it,
	// so we only assert apply succeeds and the secret exists (name + id), not the flag in state.
	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + strings.ReplaceAll(`
resource "ydb_secret" "test" {
  connection_string   = var.connection_string
  name                  = "test-secret"
  value                 = "acc-inherit-value"
  inherit_permissions   = true
}
`, "test-secret", name),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_secret.test", "name", name),
					resource.TestCheckResourceAttrSet("ydb_secret.test", "id"),
				),
			},
		},
	})
}

func TestAccYdbSecret_updateValue(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	name := "tf_acc_" + accRandomHex8(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + strings.ReplaceAll(`
resource "ydb_secret" "test" {
  connection_string = var.connection_string
  name                = "test-secret"
  value               = "first-value"
}
`, "test-secret", name),
				Check: resource.TestCheckResourceAttr("ydb_secret.test", "name", name),
			},
			{
				Config: accTestConfigPrefix(conn) + strings.ReplaceAll(`
resource "ydb_secret" "test" {
  connection_string = var.connection_string
  name                = "test-secret"
  value               = "second-value"
}
`, "test-secret", name),
				Check: resource.TestCheckResourceAttr("ydb_secret.test", "name", name),
			},
		},
	})
}

func TestAccYdbSecret_import(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	name := "tf_acc_" + accRandomHex8(t)
	cfg := accTestConfigPrefix(conn) + strings.ReplaceAll(`
resource "ydb_secret" "test" {
  connection_string = var.connection_string
  name                = "test-secret"
  value               = "import-test-value"
}
`, "test-secret", name)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: cfg,
				Check:  resource.TestCheckResourceAttr("ydb_secret.test", "name", name),
			},
			{
				ResourceName:            "ydb_secret.test",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"value", "inherit_permissions"},
			},
		},
	})
}

func TestAccYdbSecret_dataSource(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	name := "tf_acc_" + accRandomHex8(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + strings.ReplaceAll(`
resource "ydb_secret" "src" {
  connection_string = var.connection_string
  name                = "test-secret"
  value               = "ds-value"
}

data "ydb_secret" "out" {
  connection_string = ydb_secret.src.connection_string
  name              = ydb_secret.src.name
}
`, "test-secret", name),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttrPair("data.ydb_secret.out", "name", "ydb_secret.src", "name"),
					resource.TestCheckResourceAttrPair("data.ydb_secret.out", "connection_string", "ydb_secret.src", "connection_string"),
					resource.TestCheckResourceAttrPair("data.ydb_secret.out", "path", "ydb_secret.src", "path"),
				),
			},
		},
	})
}

func TestAccYdbSecret_command(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	name := "tf_acc_" + accRandomHex8(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + strings.ReplaceAll(`
resource "ydb_secret" "test" {
  connection_string = var.connection_string
  name                = "test-secret"

  command {
    path = "/bin/sh"
    args = ["-c", "printf 'from-cmd'"]
  }
}
`, "test-secret", name),
				Check: resource.TestCheckResourceAttr("ydb_secret.test", "name", name),
			},
		},
	})
}
