package terraform_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

// Acceptance tests for the ydb_topic metrics_level attribute (see acc_test.go for env and how to run).
// Requires a YDB server with topic metrics_level support (server proto field added April 2026).

func TestAccYdbTopic_metricsLevel(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	topicPath := "tf_acc_topic_metrics_" + accRandomHex8(t)

	configWithLevel := func(level int) string {
		return accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_topic" "test" {
  database_endpoint = var.connection_string
  name              = %q

  metrics_level = %d
}
`, topicPath, level)
	}

	configWithoutLevel := accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_topic" "test" {
  database_endpoint = var.connection_string
  name              = %q
}
`, topicPath)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: configWithLevel(3),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_topic.test", "name", topicPath),
					resource.TestCheckResourceAttrSet("ydb_topic.test", "id"),
					resource.TestCheckResourceAttr("ydb_topic.test", "metrics_level", "3"),
				),
			},
			{
				Config: configWithLevel(5),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_topic.test", "metrics_level", "5"),
				),
			},
			{
				// Removing metrics_level from HCL triggers AlterWithResetMetricsLevel,
				// which reverts the topic to the database default (read back as 0).
				Config: configWithoutLevel,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_topic.test", "metrics_level", "0"),
				),
			},
			{
				// After a reset, metrics_level can be set again (AlterWithSetMetricsLevel).
				Config: configWithLevel(7),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_topic.test", "metrics_level", "7"),
				),
			},
		},
	})
}
