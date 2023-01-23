package ydb

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/terraform"
)

func Provider() *schema.Provider {
	return terraform.Provider()
}
