package ydb

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb/terraform-provider-ydb/internal/resources/table"
)

func resourceYdbTable() *schema.Resource {
	return table.ResourceYDBTable()
}
