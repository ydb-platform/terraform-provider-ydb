package externaldatasource

import (
	"context"
	"net/url"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type mutationKind uint8

const (
	createMutation mutationKind = iota
	replaceMutation
)

func (h *Handler) mutate(ctx context.Context, d *schema.ResourceData, meta interface{}, kind mutationKind) diag.Diagnostics {
	r, err := resourceSchemaToResource(d)
	if err != nil {
		return diag.FromErr(err)
	}
	if err := validateSourceType(r, nil); err != nil {
		return diag.FromErr(err)
	}
	if kind == replaceMutation && r.Entity == nil {
		return diag.Errorf("external data source id is required for update")
	}
	db, err := h.open(ctx, r.getConnectionString())
	if err != nil {
		return diag.Errorf("failed to initialize client: %s", err)
	}
	defer func() { _ = db.Close(ctx) }()

	var fullPath string
	action := "update"
	if kind == createMutation {
		databaseURL, err := url.Parse(d.Get("connection_string").(string))
		if err != nil {
			return diag.Errorf("failed to parse database endpoint: %s", err)
		}
		fullPath = databaseURL.Query().Get("database") + "/" + r.Path
		action = "create"
	} else {
		fullPath = r.Entity.GetFullEntityPath()
	}
	if err := db.Exec(ctx, PrepareDataSourceQuery(fullPath, r)); err != nil {
		return diag.Errorf("failed to %s external data source: %s", action, err)
	}
	if kind == createMutation {
		d.SetId(d.Get("connection_string").(string) + "?path=" + r.Path)
	}
	return h.Read(ctx, d, meta)
}
