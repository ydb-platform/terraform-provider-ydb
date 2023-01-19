package topic

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccYcpYDBTopic_basic(t *testing.T) {
	t.Parallel()

	ydbResourceName := fmt.Sprintf("ydb-topic-permissions-test-%s", acctest.RandString(5))
	topicName := fmt.Sprintf("permissions-test-%s", acctest.RandString(5))
	topicResourceName := fmt.Sprintf("ydb-test-topic-%s", acctest.RandString(5))

	existingYDBResourceName := fmt.Sprintf("ycp_ydb_database.%s", ydbResourceName)
	existingTopicResourceName := fmt.Sprintf("ycp_ydb_topic.%s", topicResourceName)
	resource.Test(t, resource.TestCase{
		Steps: []resource.TestStep{
			{
				Check: resource.ComposeTestCheckFunc(
					testAccYcpYDBTopicExist(topicName, existingYDBResourceName, existingTopicResourceName),
				),
			},
			{
				ResourceName:      "ydb_topic.topic1",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func testAccYcpYDBTopicExist(topicPath, ydbResourceName, topicResourceName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		// TODO(shmel1k@): remove copypaste there and in ydb_permissions_test
		prs, ok := s.RootModule().Resources[topicResourceName]
		if !ok {
			return fmt.Errorf("not found: %s", topicResourceName)
		}
		if prs.Primary.ID == "" {
			return fmt.Errorf("%s", "no ID for permission is set")
		}

		rs, ok := s.RootModule().Resources[ydbResourceName]
		if !ok {
			return fmt.Errorf("not found: %s", ydbResourceName)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("no ID is set")
		}

		_, _, _, err := parseYDBDatabaseEndpoint(rs.Primary.Attributes["endpoint"])
		if err != nil {
			return err
		}

		return nil

	}
}
