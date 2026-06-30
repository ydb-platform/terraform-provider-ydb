package table

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/resources/table"
)

// CustomizeDiff rejects unsupported column schema changes at plan time.
func CustomizeDiff(_ context.Context, d *schema.ResourceDiff, _ interface{}) error {
	return table.ValidateResourceDiffColumns(d)
}
