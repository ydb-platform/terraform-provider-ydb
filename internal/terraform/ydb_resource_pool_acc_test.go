package terraform_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

// These acceptance tests require a YDB cluster with EnableResourcePools enabled.

func TestAccYdbResourcePool_withClassifier(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	suffix := accRandomHex8(t)
	poolName := "tf_acc_pool_" + suffix
	classifierName := "tf_acc_classifier_" + suffix
	memberName := "tf_acc_member_" + suffix

	// Keep the random rank below 2^53 so it round-trips exactly through Terraform's
	// JSON number representation used by the legacy plugin SDK.
	rank, err := strconv.ParseInt(suffix[:12], 16, 64)
	if err != nil {
		t.Fatalf("parse classifier rank: %v", err)
	}

	initialConfig := accTestConfigPrefix(conn) + fmt.Sprintf(`
locals {
  resource_pool_test_suffix     = %q
  resource_pool_name            = "tf_acc_pool_${local.resource_pool_test_suffix}"
  resource_pool_classifier_name = "tf_acc_classifier_${local.resource_pool_test_suffix}"
  resource_pool_member_name     = "tf_acc_member_${local.resource_pool_test_suffix}"
}

resource "ydb_resource_pool" "test" {
  connection_string                   = var.connection_string
  name                                = local.resource_pool_name
  concurrent_query_limit              = 4
  queue_size                          = 16
  database_load_cpu_threshold         = 80
  resource_weight                     = 100
  total_cpu_limit_percent_per_node    = 70.5
  query_cpu_limit_percent_per_node    = 35.5
}

resource "ydb_resource_pool_classifier" "test" {
  connection_string = var.connection_string
  name              = local.resource_pool_classifier_name
  resource_pool     = ydb_resource_pool.test.name
  member_name       = local.resource_pool_member_name
}
`, suffix)

	updatedConfig := accTestConfigPrefix(conn) + fmt.Sprintf(`
locals {
  resource_pool_test_suffix     = %q
  resource_pool_name            = "tf_acc_pool_${local.resource_pool_test_suffix}"
  resource_pool_classifier_name = "tf_acc_classifier_${local.resource_pool_test_suffix}"
  resource_pool_member_name     = "tf_acc_member_${local.resource_pool_test_suffix}"
}

resource "ydb_resource_pool" "test" {
  connection_string                   = var.connection_string
  name                                = local.resource_pool_name
  concurrent_query_limit              = 8
  queue_size                          = 32
  database_load_cpu_threshold         = 75
  resource_weight                     = 50
  total_cpu_limit_percent_per_node    = 60.5
  query_cpu_limit_percent_per_node    = 30.5
}

resource "ydb_resource_pool_classifier" "test" {
  connection_string = var.connection_string
  name              = local.resource_pool_classifier_name
  rank              = %d
  resource_pool     = ydb_resource_pool.test.name
  member_name       = local.resource_pool_member_name
}
`, suffix, rank+1)

	resetMemberConfig := accTestConfigPrefix(conn) + fmt.Sprintf(`
locals {
  resource_pool_test_suffix     = %q
  resource_pool_name            = "tf_acc_pool_${local.resource_pool_test_suffix}"
  resource_pool_classifier_name = "tf_acc_classifier_${local.resource_pool_test_suffix}"
}

resource "ydb_resource_pool" "test" {
  connection_string                   = var.connection_string
  name                                = local.resource_pool_name
  concurrent_query_limit              = 8
  queue_size                          = 32
  database_load_cpu_threshold         = 75
  resource_weight                     = 50
  total_cpu_limit_percent_per_node    = 60.5
  query_cpu_limit_percent_per_node    = 30.5
}

resource "ydb_resource_pool_classifier" "test" {
  connection_string = var.connection_string
  name              = local.resource_pool_classifier_name
  rank              = %d
  resource_pool     = ydb_resource_pool.test.name
}
`, suffix, rank+1)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: initialConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "name", poolName),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "concurrent_query_limit", "4"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "queue_size", "16"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "database_load_cpu_threshold", "80"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "resource_weight", "100"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "total_cpu_limit_percent_per_node", "70.5"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "query_cpu_limit_percent_per_node", "35.5"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "total_memory_limit_percent_per_node", "-1"),
					resource.TestCheckResourceAttrSet("ydb_resource_pool.test", "id"),
					resource.TestCheckResourceAttr("ydb_resource_pool_classifier.test", "name", classifierName),
					resource.TestCheckResourceAttrSet("ydb_resource_pool_classifier.test", "rank"),
					resource.TestCheckResourceAttr("ydb_resource_pool_classifier.test", "resource_pool", poolName),
					resource.TestCheckResourceAttr("ydb_resource_pool_classifier.test", "member_name", memberName),
					resource.TestCheckResourceAttrSet("ydb_resource_pool_classifier.test", "id"),
				),
			},
			{
				Config: updatedConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "concurrent_query_limit", "8"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "queue_size", "32"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "database_load_cpu_threshold", "75"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "resource_weight", "50"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "total_cpu_limit_percent_per_node", "60.5"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "query_cpu_limit_percent_per_node", "30.5"),
					resource.TestCheckResourceAttr("ydb_resource_pool.test", "total_memory_limit_percent_per_node", "-1"),
					resource.TestCheckResourceAttr("ydb_resource_pool_classifier.test", "rank", strconv.FormatInt(rank+1, 10)),
				),
			},
			{
				Config: resetMemberConfig,
				Check: resource.TestCheckResourceAttr(
					"ydb_resource_pool_classifier.test",
					"member_name",
					"",
				),
			},
			{
				ResourceName:      "ydb_resource_pool.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				ResourceName:      "ydb_resource_pool_classifier.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config:   resetMemberConfig,
				PlanOnly: true,
			},
		},
	})
}
