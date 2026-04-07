package externaldatasource

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func (h *Handler) Update(_ context.Context, _ *schema.ResourceData, _ interface{}) diag.Diagnostics {
	return diag.Errorf("update is not supported for external data source, all changes require recreation")
}
