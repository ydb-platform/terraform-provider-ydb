package externaltable

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/internal/resources/externaltable"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func ResourceCreateFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{Severity: diag.Error, Summary: "failed to create token for YDB request", Detail: err.Error()},
			}
		}
		h := externaltable.NewHandler(authCreds)
		return h.Create(ctx, d, meta)
	}
}

func ResourceReadFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{Severity: diag.Error, Summary: "failed to create token for YDB request", Detail: err.Error()},
			}
		}
		h := externaltable.NewHandler(authCreds)
		return h.Read(ctx, d, meta)
	}
}

func ResourceDeleteFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{Severity: diag.Error, Summary: "failed to create token for YDB request", Detail: err.Error()},
			}
		}
		h := externaltable.NewHandler(authCreds)
		return h.Delete(ctx, d, meta)
	}
}

func DataSourceReadFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{Severity: diag.Error, Summary: "failed to create token for YDB request", Detail: err.Error()},
			}
		}
		h := externaltable.NewHandler(authCreds)
		return h.DataSourceRead(ctx, d, meta)
	}
}

var externalTableFormats = []string{
	"csv_with_names",
	"tsv_with_names",
	"json_list",
	"json_each_row",
	"parquet",
	"raw",
}

func ResourceSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		"connection_string": {
			Type:        schema.TypeString,
			Description: "Connection string for the database.",
			Required:    true,
			ForceNew:    true,
		},
		"path": {
			Type:         schema.TypeString,
			Description:  "Path to the external table.",
			Required:     true,
			ForceNew:     true,
			ValidateFunc: helpers.YdbTablePathCheck,
		},
		"column": {
			Type:        schema.TypeList,
			Description: "A list of column definitions.",
			Required:    true,
			ForceNew:    true,
			Elem: &schema.Resource{
				Schema: map[string]*schema.Schema{
					"name": {
						Type:         schema.TypeString,
						Description:  "Column name.",
						Required:     true,
						ValidateFunc: validation.NoZeroValues,
					},
					"type": {
						Type:         schema.TypeString,
						Description:  "Column data type (YQL type).",
						Required:     true,
						ValidateFunc: validation.NoZeroValues,
					},
					"not_null": {
						Type:        schema.TypeBool,
						Description: "Column cannot be NULL.",
						Optional:    true,
						Default:     false,
					},
				},
			},
		},
		"data_source_path": {
			Type:        schema.TypeString,
			Description: "Name of the external data source.",
			Required:    true,
			ForceNew:    true,
		},
		"location": {
			Type:        schema.TypeString,
			Description: "Path within the external data source.",
			Required:    true,
			ForceNew:    true,
		},
		"format": {
			Type:             schema.TypeString,
			Description:      "Data format (csv_with_names, tsv_with_names, json_list, json_each_row, parquet, raw).",
			Required:         true,
			ForceNew:         true,
			ValidateDiagFunc: validation.ToDiagFunc(validation.StringInSlice(externalTableFormats, false)),
		},
		"compression": {
			Type:        schema.TypeString,
			Description: "Compression algorithm (e.g. gzip).",
			Optional:    true,
			ForceNew:    true,
		},
	}
}

func DataSourceSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		"connection_string": {
			Type:        schema.TypeString,
			Description: "Connection string for the database.",
			Required:    true,
		},
		"path": {
			Type:        schema.TypeString,
			Description: "Path to the external table.",
			Required:    true,
		},
		"column": {
			Type:        schema.TypeList,
			Description: "A list of column definitions.",
			Computed:    true,
			Elem: &schema.Resource{
				Schema: map[string]*schema.Schema{
					"name": {
						Type:        schema.TypeString,
						Description: "Column name.",
						Computed:    true,
					},
					"type": {
						Type:        schema.TypeString,
						Description: "Column data type.",
						Computed:    true,
					},
					"not_null": {
						Type:        schema.TypeBool,
						Description: "Column cannot be NULL.",
						Computed:    true,
					},
				},
			},
		},
		"data_source_path": {
			Type:        schema.TypeString,
			Description: "Name of the external data source.",
			Computed:    true,
		},
		"location": {
			Type:        schema.TypeString,
			Description: "Path within the external data source.",
			Computed:    true,
		},
		"format": {
			Type:        schema.TypeString,
			Description: "Data format.",
			Computed:    true,
		},
		"compression": {
			Type:        schema.TypeString,
			Description: "Compression algorithm.",
			Computed:    true,
		},
	}
}
