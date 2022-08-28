package table

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func TableCreate(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
	return nil
}
