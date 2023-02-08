package changefeed

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers/topic"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func prepareDropParams(d *schema.ResourceData) dropCDCParams {
	var databaseEndpoint string
	if d.HasChange("database_endpoint") {
		old, _ := d.GetChange("database_endpoint")
		databaseEndpoint = old.(string)
	} else {
		databaseEndpoint = d.Get("database_endpoint").(string)
	}

	var tablePath string
	if d.HasChange("table_path") {
		old, _ := d.GetChange("table_path")
		tablePath = old.(string)
	} else {
		tablePath = d.Get("table_path").(string)
	}

	var name string
	if d.HasChange("name") {
		old, _ := d.GetChange("name")
		name = old.(string)
	} else {
		name = d.Get("name").(string)
	}

	return dropCDCParams{
		name:             name,
		databaseEndpoint: databaseEndpoint,
		tablePath:        tablePath,
	}
}

func (h *handler) Update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cdcResource, err := changefeedResourceSchemaToChangefeedResource(d)
	if err != nil {
		return diag.FromErr(err)
	}

	if d.HasChangeExcept("consumer") {
		// TODO(shmel1k@): improve deletion behavior.
		params := prepareDropParams(d)
		err := h.dropCDC(ctx, params)
		if err != nil {
			return err
		}

		d.SetId("")

		return h.Create(ctx, d, meta)
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

	topicPath := cdcResource.TablePath + "/" + cdcResource.Name
	desc, err := db.Topic().Describe(ctx, topicPath)
	if err != nil {
		return diag.FromErr(err)
	}

	alterConsumersOptions := topic.MergeConsumerSettings(d.Get("consumer").([]interface{}), desc.Consumers)
	err = db.Topic().Alter(ctx, topicPath, alterConsumersOptions...)
	if err != nil {
		return diag.FromErr(err)
	}

	return h.Read(ctx, d, meta)
}
