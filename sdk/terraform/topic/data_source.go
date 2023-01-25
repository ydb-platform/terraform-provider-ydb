package topic

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func DataSourceReadFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		token, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}
		c := &caller{
			token: token,
		}
		return c.dataSourceYDBTopicRead(ctx, d, meta)
	}
}

func (c *caller) dataSourceYDBTopicRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	_ = meta

	client, err := c.createYDBConnection(ctx, d, nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-stream control plane client: %w", err))
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	description, err := client.Topic().Describe(ctx, d.Get("name").(string))
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			// stream was deleted outside from terraform.
			d.SetId("")
			return nil
		}
		return diag.FromErr(fmt.Errorf("datasource: failed to describe stream: %w", err))
	}

	err = flattenYDBTopicDescription(d, description)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to flatten stream description: %w", err))
	}

	return nil
}
