package table

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/sosodev/duration"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func (h *handler) Create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	tableResource, err := tableResourceSchemaToTableResource(d)
	if err != nil {
		return diag.FromErr(err)
	}
	if tableResource == nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "got nil resource, unreachable code",
			},
		}
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

	q := PrepareCreateRequest(tableResource)
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		var (
			ttlOpt options.TimeToLiveSettings
			dur    *duration.Duration
			ttlSet bool
		)
		if tableResource.TTL != nil && !isTTLYqlType(tableResource) {
			if dur, err = duration.Parse(tableResource.TTL.ExpireInterval); err != nil {
				return
			}
			switch tableResource.TTL.Unit {
			case "UNIT_SECONDS":
				ttlOpt = options.NewTTLSettings().ColumnSeconds(tableResource.TTL.ColumnName)
			case "UNIT_MILLISECONDS":
				ttlOpt = options.NewTTLSettings().ColumnMilliseconds(tableResource.TTL.ColumnName)
			case "UNIT_MICROSECONDS":
				ttlOpt = options.NewTTLSettings().ColumnMicroseconds(tableResource.TTL.ColumnName)
			case "UNIT_NANOSECONDS":
				ttlOpt = options.NewTTLSettings().ColumnNanoseconds(tableResource.TTL.ColumnName)
			default:
				return fmt.Errorf("wrong ttl unit: %s", tableResource.TTL.Unit)
			}
			ttlSet = true
		}
		if err = s.ExecuteSchemeQuery(ctx, q); err != nil {
			return
		}
		if ttlSet {
			err = s.AlterTable(ctx, tableResource.FullPath,
				options.WithSetTimeToLiveSettings(
					ttlOpt.ExpireAfter(dur.ToTimeDuration()),
				),
			)
		}
		return
	})
	if err != nil {
		return diag.Diagnostics{
			{
				Severity: diag.Error,
				Summary:  "failed to create table",
				Detail:   err.Error(),
			},
		}
	}

	id := tableResource.DatabaseEndpoint + "?path=" + tableResource.Path
	d.SetId(id)

	return h.Read(ctx, d, meta)
}
