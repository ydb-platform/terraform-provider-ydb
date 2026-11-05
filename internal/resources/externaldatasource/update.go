package externaldatasource

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *Handler) Update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	if diags := guardIAMMutation(d); diags.HasError() {
		return diags
	}
	r, err := resourceSchemaToResource(d)
	if err != nil {
		return diag.FromErr(err)
	}
	if err := validateSourceType(r, nil); err != nil {
		return diag.FromErr(err)
	}
	if r.strAttr("auth_method") == "IAM" && h.authCreds.Token == "" {
		return diag.Errorf("IAM authentication requires the caller's IAM token")
	}

	if r.Entity == nil {
		return diag.Errorf("external data source id is required for update")
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: r.getConnectionString(),
		AuthCreds:        h.authCreds,
	})
	if err != nil {
		return diag.Errorf("failed to initialize client: %s", iamStatus(err))
	}
	defer func() { _ = db.Close(ctx) }()
	if diags := recoverIAMSecret(ctx, d, db); diags.HasError() {
		return diags
	}
	if r.strAttr("auth_method") == "IAM" {
		r.initialTokenSecretPath, err = h.createIAMSecret(ctx, db, r.Entity.GetDatabasePath())
		if err != nil {
			return diag.FromErr(err)
		}
	}

	q := PrepareDataSourceQuery(r.Entity.GetFullEntityPath(), r)
	if ctx.Err() != nil {
		if previousAuth, _ := d.GetChange("auth_method"); r.initialTokenSecretPath != "" || previousAuth == "IAM" {
			return cancelIAMSubmission(ctx, d, db, r.initialTokenSecretPath, d.Timeout(schema.TimeoutUpdate))
		}
	}
	err = db.Query().Exec(ctx, q)
	if err != nil && r.initialTokenSecretPath == "" {
		if previousAuth, _ := d.GetChange("auth_method"); previousAuth == "IAM" {
			return completeIAMSecret(ctx, d, db, "", err)
		}
		return diag.Errorf("failed to update external data source: %s", iamStatus(err))
	}
	var diags diag.Diagnostics
	if r.initialTokenSecretPath != "" {
		diags = completeIAMSecret(ctx, d, db, r.initialTokenSecretPath, err)
		if diags.HasError() {
			return diags
		}
		return append(diags, readIAMAfterWrite(ctx, d, db)...)
	}
	return append(diags, h.Read(ctx, d, meta)...)
}
