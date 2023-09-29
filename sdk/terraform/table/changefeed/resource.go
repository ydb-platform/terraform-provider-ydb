package changefeed

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers/topic"
	"github.com/ydb-platform/terraform-provider-ydb/internal/resources/changefeed"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func ResourceCreateFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
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

		h := changefeed.NewHandler(token)
		return h.Create(ctx, d, meta)
	}
}

func ResourceReadFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
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

		h := changefeed.NewHandler(token)
		return h.Read(ctx, d, meta)
	}
}

func ResourceUpdateFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
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

		h := changefeed.NewHandler(token)
		return h.Update(ctx, d, meta)
	}
}

func ResourceDeleteFunc(cb auth.GetTokenCallback) helpers.TerraformCRUD {
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

		h := changefeed.NewHandler(token)
		return h.Delete(ctx, d, meta)
	}
}

func ResourceSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		"table_path": {
			Type:         schema.TypeString,
			Optional:     true,
			Computed:     true,
			ValidateFunc: validation.All(validation.NoZeroValues, helpers.YdbTablePathCheck),
			ForceNew:     true,
			ConflictsWith: []string{
				"table_id",
			},
		},
		"connection_string": {
			Type:         schema.TypeString,
			Optional:     true,
			Computed:     true,
			ValidateFunc: validation.NoZeroValues,
			ForceNew:     true,
			ConflictsWith: []string{
				"table_id",
			},
		},
		"table_id": {
			Type:     schema.TypeString,
			Optional: true,
			ForceNew: true,
			Computed: true,
			ConflictsWith: []string{
				"table_path",
				"connection_string",
			},
		},
		"name": {
			Type:         schema.TypeString,
			Required:     true,
			ValidateFunc: validation.NoZeroValues,
			ForceNew:     true,
		},
		"mode": {
			Type:         schema.TypeString,
			Required:     true,
			ValidateFunc: validation.NoZeroValues,
			ForceNew:     true,
		},
		"format": {
			Type:     schema.TypeString,
			Required: true,
			ForceNew: true,
		},
		"virtual_timestamps": {
			Type:     schema.TypeBool,
			Optional: true,
			ForceNew: true,
		},
		"retention_period": {
			Type:         schema.TypeString,
			Optional:     true,
			ValidateFunc: validation.NoZeroValues,
			ForceNew:     true,
		},
		"consumer": {
			Type:     schema.TypeSet,
			Optional: true,
			Computed: true,
			Elem: &schema.Resource{
				Schema: map[string]*schema.Schema{
					"name": {
						Type:         schema.TypeString,
						Required:     true,
						ValidateFunc: validation.NoZeroValues,
					},
					"supported_codecs": {
						Type:     schema.TypeList,
						Optional: true,
						Elem: &schema.Schema{
							Type:         schema.TypeString,
							ValidateFunc: validation.StringInSlice(topic.YDBTopicAllowedCodecs, false),
						},
						Computed: true,
					},
					"starting_message_timestamp_ms": {
						Type:     schema.TypeInt,
						Optional: true,
						Computed: true,
					},
				},
			},
		},
	}
}
