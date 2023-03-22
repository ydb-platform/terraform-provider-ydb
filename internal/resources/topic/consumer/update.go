package consumer

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func (h *handler) performYDBTopicConsumerUpdate(ctx context.Context, d *schema.ResourceData) diag.Diagnostics {
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
		return diag.Errorf("failed to initialize table client: %s", err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	if d.HasChange("name") {
		// Creating new topic
		return h.Create(ctx, d, nil)
	}

	topicName := tableResource.Entity.GetEntityPath()
	desc, err := db.Topic().Describe(ctx, topicName)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return h.Create(ctx, d, nil)
		}
		return diag.FromErr(fmt.Errorf("failed to get description for topic %q", topicName))
	}

	opts := prepareYDBTopicConsumerAlterSettings(d, desc)
	err = db.Topic().Alter(ctx, topicName, opts...)
	if err != nil {
		return diag.FromErr(fmt.Errorf("got error when tried to alter topic: %w", err))
	}

	return h.Read(ctx, d, nil)
}

func (h *handler) Update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	_ = meta
	return h.performYDBTopicConsumerUpdate(ctx, d)
}
