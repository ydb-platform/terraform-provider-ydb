package table

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func prepareAlterRequest(tableName string, d *schema.ResourceData, desc options.Description) (string, error) {
	diff, err := prepareTableDiff(d, desc)
	if err != nil {
		return "", err
	}
	diff.TableName = tableName

	query := PrepareAlterRequest(diff)
	return query, nil
}

func (h *handler) Update(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
	tableResource, err := tableResourceSchemaToTableResource(d)
	if err != nil {
		return diag.FromErr(err)
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: tableResource.DatabaseEndpoint,
		Token:            h.token,
	})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "failed to initialize table client",
				Detail:   err.Error(),
			},
		}
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	// TODO(shmel1k@): remove copypaste
	var description options.Description
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		description, err = s.DescribeTable(
			ctx,
			tableResource.Entity.GetFullEntityPath(),
			options.WithPartitionStats(),
			options.WithShardKeyBounds(),
			options.WithTableStats(),
		)
		return err
	})
	if err != nil {
		if strings.Contains(err.Error(), "SCHEME_ERROR") {
			d.SetId("")
			return nil
		}
		return diag.FromErr(fmt.Errorf("failed to describe table %q: %w", tableResource.Path, err))
	}

	request, err := prepareAlterRequest(tableResource.Path, d, description)
	if err != nil {
		return diag.FromErr(err)
	}

	// NOTE(shmel1k@): no query after all checks.
	if request == "" {
		return nil
	}

	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		err = s.ExecuteSchemeQuery(ctx, request)
		return err
	})
	if err != nil {
		return diag.FromErr(err)
	}
	return nil
}
