package externaldatasource

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *Handler) Read(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}
	return h.readExternalDataSource(ctx, d, entity)
}

func (h *Handler) DataSourceRead(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	databaseEndpoint := d.Get("connection_string").(string)
	path := helpers.TrimPath(d.Get("path").(string))

	if databaseEndpoint == "" || path == "" {
		return diag.FromErr(fmt.Errorf("connection_string and path are required"))
	}

	entity, err := helpers.ParseYDBEntityID(databaseEndpoint + "?path=" + path)
	if err != nil {
		return diag.FromErr(err)
	}

	diags := h.readExternalDataSource(ctx, d, entity)
	if diags.HasError() {
		return diags
	}
	d.SetId(entity.ID())
	return diags
}

func (h *Handler) readExternalDataSource(ctx context.Context, d *schema.ResourceData, entity *helpers.YDBEntity) diag.Diagnostics {
	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: entity.PrepareFullYDBEndpoint(),
		AuthCreds:        h.authCreds,
	})
	if err != nil {
		return diag.Errorf("failed to initialize client: %s", err)
	}
	defer func() { _ = db.Close(ctx) }()

	desc, err := db.Table().DescribeExternalDataSource(ctx, entity.GetFullEntityPath())
	if err != nil {
		if ydb.IsOperationErrorSchemeError(err) {
			d.SetId("")
			return nil
		}
		return diag.Errorf("failed to describe external data source %q: %s", entity.GetEntityPath(), err)
	}

	return diag.FromErr(flattenDescription(d, entity, desc.Properties, desc.SourceType, desc.Location))
}
