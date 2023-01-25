package helpers

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type TerraformCRUD func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics
