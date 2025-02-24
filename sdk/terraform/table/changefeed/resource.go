package changefeed

import (
	"context"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers/topic"
	"github.com/ydb-platform/terraform-provider-ydb/internal/resources/changefeed"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func ResourceCreateFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}

		h := changefeed.NewHandler(authCreds)
		return h.Create(ctx, d, meta)
	}
}

func ResourceReadFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}

		h := changefeed.NewHandler(authCreds)
		return h.Read(ctx, d, meta)
	}
}

func ResourceUpdateFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}

		h := changefeed.NewHandler(authCreds)
		return h.Update(ctx, d, meta)
	}
}

func ResourceDeleteFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{
					Severity: diag.Error,
					Summary:  "failed to create token for YDB request",
					Detail:   err.Error(),
				},
			}
		}

		h := changefeed.NewHandler(authCreds)
		return h.Delete(ctx, d, meta)
	}
}

func ResourceImportFunc(ctx context.Context, d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return nil, err
	}
	pathParts := strings.Split(entity.ID(), "/")
	tableIDPath := strings.Join(pathParts[:len(pathParts)-1], "/")
	resName := pathParts[len(pathParts)-1]
	if err := d.Set("table_id", tableIDPath); err != nil {
		return nil, err
	}
	if err := d.Set("name", resName); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
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
			Type:     schema.TypeList,
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
					"important": {
						Type:     schema.TypeBool,
						Optional: true,
						Computed: true,
					},
				},
			},
		},
	}
}
