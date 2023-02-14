package index

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *handler) Read(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	indexResource, err := indexResourceSchemaToIndexResource(d)
	if err != nil {
		return diag.FromErr(err)
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: indexResource.ConnectionString,
		Token:            h.token,
	})
	if err != nil {
		return diag.FromErr(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	var description options.Description
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		description, err = s.DescribeTable(
			ctx,
			parseTablePathFromIndexEntity(indexResource.Entity.GetFullEntityPath()),
			options.WithTableStats(),
		)
		return err
	})
	if err != nil {
		return diag.FromErr(err)
	}
	var indexDescription options.IndexDescription
	for _, v := range description.Indexes {
		if v.Name == indexResource.Name {
			indexDescription = v
			break
		}
	}

	if indexDescription.Name == "" {
		d.SetId("")
		return h.Create(ctx, d, meta)
	}

	prefix := "grpc://"
	if db.Secure() {
		prefix = "grpcs://"
	}

	flattenIndexDescription(d, indexResource.TablePath, indexDescription, prefix+db.Endpoint()+"/?database="+db.Name())
	return nil
}
