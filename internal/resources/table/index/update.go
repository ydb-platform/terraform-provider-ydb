package index

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func prepareDropIndexParams(d *schema.ResourceData) dropIndexParams {
	var connectionString string
	if d.HasChange("connection_string") {
		old, _ := d.GetChange("connection_string")
		connectionString = old.(string)
	} else {
		connectionString = d.Get("connection_string").(string)
	}

	var tablePath string
	if d.HasChange("table_path") {
		old, _ := d.GetChange("table_path")
		tablePath = old.(string)
	} else {
		tablePath = d.Get("table_path").(string)
	}

	var name string
	if d.HasChange("name") {
		old, _ := d.GetChange("name")
		name = old.(string)
	} else {
		name = d.Get("name").(string)
	}
	return dropIndexParams{
		name:             name,
		databaseEndpoint: connectionString,
		tablePath:        tablePath,
	}
}

func (h *handler) Update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	err1 := h.dropIndex(ctx, prepareDropIndexParams(d))
	if err1 != nil {
		return err1
	}

	d.SetId("")

	return h.Create(ctx, d, meta)
}
