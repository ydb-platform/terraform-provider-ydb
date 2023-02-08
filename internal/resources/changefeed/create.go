package changefeed

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func prepareCDCAlterQuery(tablePath string, cdc *ChangeDataCaptureSettings) string {
	return ""
}

func (h *handler) Create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cdcResource, err := changefeedResourceSchemaToChangefeedResource(d)
	if err != nil {
		return diag.FromErr(err)
	}

	if cdcResource == nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "got nil resource, unreachable code",
			},
		}
	}
	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: cdcResource.DatabaseEndpoint,
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

	q := PrepareCreateRequest(cdcResource)
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, q)
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

	opts := make([]topicoptions.AlterOption, 0, len(cdcResource.Consumers))
	for _, v := range cdcResource.Consumers {
		opts = append(opts, topicoptions.AlterWithAddConsumers(topictypes.Consumer{
			Name:            v.Name,
			Important:       v.Important,
			SupportedCodecs: v.SupportedCodecs,
			ReadFrom:        v.ReadFrom,
			Attributes:      v.Attributes,
		}))
	}

	err = db.Topic().Alter(ctx, cdcResource.TablePath+"/"+cdcResource.Name, opts...)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(cdcResource.DatabaseEndpoint + "/" + cdcResource.TablePath + "/" + cdcResource.Name)

	return h.Read(ctx, d, meta)
}
