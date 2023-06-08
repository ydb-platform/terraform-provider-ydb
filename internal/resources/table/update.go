package table

import (
	"context"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func prepareAlterRequest(tableName string, d *schema.ResourceData) (string, error) {
	diff, err := prepareTableDiff(d)
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
		DatabaseEndpoint: tableResource.getConnectionString(),
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

	request, err := prepareAlterRequest(strings.Trim(tableResource.Path, "/"), d)
	if err != nil {
		return diag.FromErr(err)
	}

	ttlOpt, isIntegralTTL, dur, errTTL := integralTTL(tableResource)
	if errTTL != nil {
		return diag.FromErr(errTTL)
	}

	// NOTE(shmel1k@): no query after all checks.
	if request == "" && !isIntegralTTL {
		return nil
	}
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		if request != "" {
			if err = s.ExecuteSchemeQuery(ctx, request); err != nil {
				return err
			}
		}
		if isIntegralTTL {
			err = s.AlterTable(ctx, db.Name()+"/"+tableResource.Path,
				options.WithSetTimeToLiveSettings(
					ttlOpt.ExpireAfter(dur.ToTimeDuration()),
				),
			)
		}
		return err
	})
	if err != nil {
		return diag.FromErr(err)
	}
	return h.Read(ctx, d, cfg)
}
