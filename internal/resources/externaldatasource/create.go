package externaldatasource

import (
	"context"
	"net/url"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *Handler) Create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
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

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: r.getConnectionString(),
		AuthCreds:        h.authCreds,
	})
	if err != nil {
		return diag.Errorf("failed to initialize client: %s", iamStatus(err))
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
	}

	q := PrepareDataSourceQuery(fullPath, r)
	if r.initialTokenSecretPath != "" {
		q = prepareDataSourceQuery(fullPath, r, "CREATE")
	}
	if ctx.Err() != nil && r.initialTokenSecretPath != "" {
		return cancelIAMSubmission(ctx, db, r.initialTokenSecretPath, d.Timeout(schema.TimeoutCreate))
	}
	err = db.Query().Exec(ctx, q)
	if err != nil && r.initialTokenSecretPath == "" {
		return diag.Errorf("failed to create external data source: %s", iamStatus(err))
	}
	if err == nil {
		d.SetId(databaseEndpoint + "?path=" + r.Path)
	}
	var diags diag.Diagnostics
	if r.initialTokenSecretPath != "" {
		diags = completeIAMSecret(ctx, db, r.initialTokenSecretPath, err)
		if diags.HasError() {
			return diags
		}
		return append(diags, readIAMAfterWrite(ctx, d, db)...)
	}
	return append(diags, h.Read(ctx, d, meta)...)
}
