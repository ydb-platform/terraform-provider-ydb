package externaldatasource

import (
	"context"
	"fmt"
	"net/url"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/resources/secret"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *Handler) Create(ctx context.Context, d *schema.ResourceData, meta interface{}) (diags diag.Diagnostics) {
	r, err := resourceSchemaToResource(d)
	if err != nil {
		return diag.FromErr(err)
	}
	if err := validateSourceType(r, nil); err != nil {
		return diag.FromErr(err)
	}
	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: r.getConnectionString(),
		AuthCreds:        h.authCreds,
	})
	if err != nil {
		return diag.Errorf("failed to initialize client: %s", err)
	}
	defer func() { _ = db.Close(ctx) }()

	databaseEndpoint := d.Get("connection_string").(string)
	databaseURL, err := url.Parse(databaseEndpoint)
	if err != nil {
		return diag.Errorf("failed to parse database endpoint: %s", err)
	}
	fullPath := databaseURL.Query().Get("database") + "/" + r.Path
	if r.strAttr("auth_method") == "IAM" {
		r.initialTokenSecretPath, err = h.createIAMSecret(ctx, db, databaseURL.Query().Get("database"))
		if err != nil {
			return diag.FromErr(err)
		}
		defer func() {
			if err := secret.DropSecret(ctx, db, r.initialTokenSecretPath); err != nil {
				diags = append(diags, diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  fmt.Sprintf("failed to drop temporary IAM secret %q", r.initialTokenSecretPath),
					Detail:   err.Error(),
				})
			}
		}()
	}

	q := PrepareDataSourceQuery(fullPath, r)
	err = db.Query().Exec(ctx, q)
	if err != nil {
		return diag.Errorf("failed to create external data source: %s", err)
	}
	d.SetId(databaseEndpoint + "?path=" + r.Path)
	return h.Read(ctx, d, meta)
}
