package secret

import (
	"context"
	"path"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *handler) Read(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: entity.PrepareFullYDBEndpoint(),
		AuthCreds:        h.authCreds,
	})
	if err != nil {
		return diag.Diagnostics{
			{Severity: diag.Error, Summary: "failed to initialize table client", Detail: err.Error()},
		}
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	secretName := path.Base(entity.GetEntityPath())
	parentDir := path.Dir(entity.GetFullEntityPath())
	if parentDir == "." {
		parentDir = entity.GetDatabasePath()
	}

	dir, err := db.Scheme().ListDirectory(ctx, parentDir)
	if err != nil {
		if ydb.IsOperationErrorSchemeError(err) {
			d.SetId("")
			return nil
		}
		return diag.Errorf("failed to list directory %q: %s", parentDir, err)
	}

	found := false
	for _, child := range dir.Children {
		if child.Name == secretName {
			found = true
			break
		}
	}

	if !found {
		d.SetId("")
		return nil
	}

	_ = d.Set("connection_string", entity.PrepareFullYDBEndpoint())
	_ = d.Set("name", entity.GetEntityPath())

	return nil
}
