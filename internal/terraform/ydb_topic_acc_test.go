package terraform_test

import (
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

// Acceptance tests for ydb_topic (see acc_test.go for env and how to run).
// metrics_level tests require a YDB server with topic metrics_level support (server proto field added April 2026).

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

func TestAccYdbTopic_retentionPeriod_conflictWithHours(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	topicPath := "tf_acc_topic_retention_hours_conflict_" + accRandomHex8(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_topic" "test" {
  database_endpoint       = var.connection_string
  name                    = %q
  retention_period        = "24h"
  retention_period_hours  = 24
}
`, topicPath),
				ExpectError: regexp.MustCompile(`(?i)conflict`),
			},
		},
	})
}

func TestAccYdbTopic_retentionPeriod_invalidFormat(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	topicPath := "tf_acc_topic_retention_invalid_" + accRandomHex8(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_topic" "test" {
  database_endpoint = var.connection_string
  name              = %q
  retention_period  = "not-a-duration"
}
`, topicPath),
				ExpectError: regexp.MustCompile(`valid Go duration`),
			},
		},
	})
}

func TestAccYdbTopic_retentionPeriod_zeroDuration(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	topicPath := "tf_acc_topic_retention_zero_" + accRandomHex8(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_topic" "test" {
  database_endpoint = var.connection_string
  name              = %q
  retention_period  = "0s"
}
`, topicPath),
				ExpectError: regexp.MustCompile(`must be greater than zero`),
			},
		},
	})
}

func TestAccYdbTopic_retentionPeriod_describe13Hours(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	topicPath := "tf_acc_topic_retention_13h_" + accRandomHex8(t)
	want := 13 * time.Hour

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_topic" "test" {
  database_endpoint = var.connection_string
  name              = %q
  retention_period  = "13h"
}
`, topicPath),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_topic.test", "retention_period", want.String()),
					testAccCheckYdbTopicRetentionPeriod(t, topicPath, want),
				),
			},
		},
	})
}

func TestAccYdbTopic_retentionPeriod_migrateHoursToPeriod(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	topicPath := "tf_acc_topic_retention_migrate_" + accRandomHex8(t)
	want := 13 * time.Hour

	configWithHours := accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_topic" "test" {
  database_endpoint      = var.connection_string
  name                   = %q
  retention_period_hours = 13
}
`, topicPath)

	configWithPeriod := accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_topic" "test" {
  database_endpoint = var.connection_string
  name              = %q
  retention_period  = "13h"
}
`, topicPath)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: configWithHours,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("ydb_topic.test", "retention_period_hours", "13"),
					resource.TestCheckResourceAttr("ydb_topic.test", "retention_period", want.String()),
					testAccCheckYdbTopicRetentionPeriod(t, topicPath, want),
				),
			},
			{
				Config:   configWithPeriod,
				PlanOnly: true,
			},
		},
	})
}

// testAccCheckYdbTopicRetentionPeriod verifies retention via YDB Describe, independently of provider Read.
// TestCheckResourceAttr alone only checks Terraform state after Read and would pass even if Create/Alter
// did not apply the value to YDB or Read mapped the response incorrectly.
func testAccCheckYdbTopicRetentionPeriod(t *testing.T, topicPath string, want time.Duration) resource.TestCheckFunc {
	t.Helper()
	return func(s *terraform.State) error {
		db := accOpenYDB(t)
		desc, err := db.Topic().Describe(t.Context(), topicPath)
		if err != nil {
			return fmt.Errorf("describe topic %q: %w", topicPath, err)
		}
		if desc.RetentionPeriod != want {
			return fmt.Errorf("topic %q retention_period = %v, want %v", topicPath, desc.RetentionPeriod, want)
		}
		return nil
	}
}
