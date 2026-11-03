package externaldatasource

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
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
	if diags := h.recoverCleanup(ctx, d, entity.PrepareFullYDBEndpoint()); diags.HasError() {
		return diags
	}
	db, err := h.open(ctx, entity.PrepareFullYDBEndpoint())
	if err != nil {
		return diag.Errorf("failed to initialize client: %s", safeStatus(err))
	}
	defer h.close(ctx, db)

	desc, err := db.Describe(ctx, entity.GetFullEntityPath())
	if err != nil {
		if ydb.IsOperationErrorNotFoundError(err) || (d.Get("auth_method") != "IAM" && ydb.IsOperationErrorSchemeError(err)) {
			d.SetId("")
			return nil
		}
		if d.Get("auth_method") == "IAM" && ydb.IsOperationErrorSchemeError(err) {
			return reconcileReadSchemeError(ctx, db, d, entity, err)
		}
		return diag.Errorf("failed to describe external data source %q: %s", entity.GetEntityPath(), safeStatus(err))
	}

	return diag.FromErr(flattenDescription(d, entity, desc.Properties, desc.SourceType, desc.Location))
}

// A scheme error can mean missing or wrong-type; forget ownership only after an exact absence check.
func reconcileReadSchemeError(ctx context.Context, db backend, d *schema.ResourceData, entity *helpers.YDBEntity, describeErr error) diag.Diagnostics {
	absent, err := db.PathAbsent(ctx, entity.GetFullEntityPath())
	if err != nil {
		return diag.Errorf("failed to reconcile external data source %q after %s: %s; identity retained", entity.GetEntityPath(), safeStatus(describeErr), safeStatus(err))
	}
	if absent {
		d.SetId("")
		return nil
	}
	return diag.Errorf("failed to describe external data source %q: %s", entity.GetEntityPath(), safeStatus(describeErr))
}
