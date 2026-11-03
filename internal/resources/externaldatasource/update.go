package externaldatasource

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func (h *Handler) Update(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	return h.mutate(ctx, d, replaceMutation)
}
