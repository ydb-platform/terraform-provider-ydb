package externaldatasource

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/internal/resources/externaldatasource"
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
		h := externaldatasource.NewHandler(authCreds)
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
		h := externaldatasource.NewHandler(authCreds)
		return h.Read(ctx, d, meta)
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
		h := externaldatasource.NewHandler(authCreds)
		return h.DataSourceRead(ctx, d, meta)
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
		h := externaldatasource.NewHandler(authCreds)
		return h.Delete(ctx, d, meta)
	}
}

// Allowed values for YDB EXTERNAL DATA SOURCE WITH options (resource config).
var (
	externalDataSourceSourceTypes = []string{
		"ObjectStorage", "ClickHouse", "PostgreSQL", "MySQL",
		"Ydb", "YT", "Greenplum", "MsSQLServer",
		"Oracle", "Logging", "Solomon", "Redis",
		"Prometheus", "MongoDB", "OpenSearch", "YdbTopics",
	}
	externalDataSourceAuthMethods = []string{"NONE", "BASIC", "MDB_BASIC", "AWS", "TOKEN", "SERVICE_ACCOUNT"}
	externalDataSourceProtocols   = []string{"NATIVE", "HTTP"}
)

func optionalEnum(valid []string) schema.SchemaValidateDiagFunc {
	return validation.ToDiagFunc(
		validation.Any(validation.StringIsEmpty, validation.StringInSlice(valid, false)),
	)
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
			Description:  "Path to the external data source.",
			Required:     true,
			ForceNew:     true,
			ValidateFunc: helpers.YdbTablePathCheck,
		},
		"source_type": {
			Type:         schema.TypeString,
			Description:  "Type of the external data source (e.g. ObjectStorage, ClickHouse, PostgreSQL).",
			Required:     true,
			ForceNew:     true,
			ValidateFunc: validation.StringInSlice(externalDataSourceSourceTypes, false),
		},
		"location": {
			Type:        schema.TypeString,
			Description: "Network address of the external data source.",
			Required:    true,
			ForceNew:    true,
		},
		"auth_method": {
			Type:             schema.TypeString,
			Description:      "Authentication method (NONE, BASIC, MDB_BASIC, AWS, TOKEN, SERVICE_ACCOUNT).",
			Optional:         true,
			ForceNew:         true,
			ValidateDiagFunc: optionalEnum(externalDataSourceAuthMethods),
		},
		"login": {
			Type:        schema.TypeString,
			Description: "Login for BASIC/MDB_BASIC authentication.",
			Optional:    true,
			ForceNew:    true,
		},
		"password_secret_path": {
			Type:        schema.TypeString,
			Description: "Secret path for the password.",
			Optional:    true,
			ForceNew:    true,
		},
		"service_account_id": {
			Type:        schema.TypeString,
			Description: "Service account ID.",
			Optional:    true,
			ForceNew:    true,
		},
		"service_account_secret_path": {
			Type:        schema.TypeString,
			Description: "Secret path for service account authentication.",
			Optional:    true,
			ForceNew:    true,
		},
		"aws_access_key_id_secret_path": {
			Type:        schema.TypeString,
			Description: "Secret path for AWS access key ID.",
			Optional:    true,
			ForceNew:    true,
		},
		"aws_secret_access_key_secret_path": {
			Type:        schema.TypeString,
			Description: "Secret path for AWS secret access key.",
			Optional:    true,
			ForceNew:    true,
		},
		"aws_region": {
			Type:        schema.TypeString,
			Description: "AWS region.",
			Optional:    true,
			ForceNew:    true,
		},
		"token_secret_path": {
			Type:        schema.TypeString,
			Description: "Secret path for TOKEN authentication.",
			Optional:    true,
			ForceNew:    true,
		},
		"database_name": {
			Type:        schema.TypeString,
			Description: "Database name in the external data source.",
			Optional:    true,
			ForceNew:    true,
		},
		"protocol": {
			Type:             schema.TypeString,
			Description:      "Communication protocol (NATIVE, HTTP).",
			Optional:         true,
			ForceNew:         true,
			ValidateDiagFunc: optionalEnum(externalDataSourceProtocols),
		},
		"use_tls": {
			Type:        schema.TypeBool,
			Description: "Whether to use TLS in the external data source connection.",
			Optional:    true,
			ForceNew:    true,
		},
		"mdb_cluster_id": {
			Type:        schema.TypeString,
			Description: "Managed Database cluster ID.",
			Optional:    true,
			ForceNew:    true,
		},
		"schema": {
			Type:        schema.TypeString,
			Description: "Schema name (for PostgreSQL, Greenplum).",
			Optional:    true,
			ForceNew:    true,
		},
		"service_name": {
			Type:        schema.TypeString,
			Description: "Service name (for Oracle).",
			Optional:    true,
			ForceNew:    true,
		},
		"folder_id": {
			Type:        schema.TypeString,
			Description: "Folder ID (for Logging).",
			Optional:    true,
			ForceNew:    true,
		},
		"grpc_location": {
			Type:        schema.TypeString,
			Description: "gRPC location (for Solomon).",
			Optional:    true,
			ForceNew:    true,
		},
		"project": {
			Type:        schema.TypeString,
			Description: "Project name (for Solomon).",
			Optional:    true,
			ForceNew:    true,
		},
		"cluster": {
			Type:        schema.TypeString,
			Description: "Cluster name (for Solomon).",
			Optional:    true,
			ForceNew:    true,
		},
		"database_id": {
			Type:        schema.TypeString,
			Description: "Database ID (for Ydb).",
			Optional:    true,
			ForceNew:    true,
		},
		"reading_mode": {
			Type:        schema.TypeString,
			Description: "Reading mode (for MongoDB).",
			Optional:    true,
			ForceNew:    true,
		},
		"unexpected_type_display_mode": {
			Type:        schema.TypeString,
			Description: "Unexpected type display mode (for MongoDB).",
			Optional:    true,
			ForceNew:    true,
		},
		"unsupported_type_display_mode": {
			Type:        schema.TypeString,
			Description: "Unsupported type display mode (for MongoDB).",
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
			Description: "Path to the external data source.",
			Required:    true,
		},
		"source_type": {
			Type:        schema.TypeString,
			Description: "Type of the external data source.",
			Computed:    true,
		},
		"location": {
			Type:        schema.TypeString,
			Description: "Network address of the external data source.",
			Computed:    true,
		},
		"auth_method": {
			Type:        schema.TypeString,
			Description: "Authentication method.",
			Computed:    true,
		},
		"login": {
			Type:        schema.TypeString,
			Description: "Login.",
			Computed:    true,
		},
		"password_secret_path": {
			Type:        schema.TypeString,
			Description: "Secret path for the password.",
			Computed:    true,
		},
		"service_account_id": {
			Type:        schema.TypeString,
			Description: "Service account ID.",
			Computed:    true,
		},
		"service_account_secret_path": {
			Type:        schema.TypeString,
			Description: "Secret path for service account.",
			Computed:    true,
		},
		"aws_access_key_id_secret_path": {
			Type:        schema.TypeString,
			Description: "Secret path for AWS access key ID.",
			Computed:    true,
		},
		"aws_secret_access_key_secret_path": {
			Type:        schema.TypeString,
			Description: "Secret path for AWS secret access key.",
			Computed:    true,
		},
		"aws_region": {
			Type:        schema.TypeString,
			Description: "AWS region.",
			Computed:    true,
		},
		"token_secret_path": {
			Type:        schema.TypeString,
			Description: "Secret path for TOKEN authentication.",
			Computed:    true,
		},
		"database_name": {
			Type:        schema.TypeString,
			Description: "Database name in the external data source.",
			Computed:    true,
		},
		"protocol": {
			Type:        schema.TypeString,
			Description: "Communication protocol.",
			Computed:    true,
		},
		"use_tls": {
			Type:        schema.TypeBool,
			Description: "Whether TLS is used.",
			Computed:    true,
		},
		"mdb_cluster_id": {
			Type:        schema.TypeString,
			Description: "Managed Database cluster ID.",
			Computed:    true,
		},
		"schema": {
			Type:        schema.TypeString,
			Description: "Schema name.",
			Computed:    true,
		},
		"service_name": {
			Type:        schema.TypeString,
			Description: "Service name.",
			Computed:    true,
		},
		"folder_id": {
			Type:        schema.TypeString,
			Description: "Folder ID.",
			Computed:    true,
		},
		"grpc_location": {
			Type:        schema.TypeString,
			Description: "gRPC location.",
			Computed:    true,
		},
		"project": {
			Type:        schema.TypeString,
			Description: "Project name.",
			Computed:    true,
		},
		"cluster": {
			Type:        schema.TypeString,
			Description: "Cluster name.",
			Computed:    true,
		},
		"database_id": {
			Type:        schema.TypeString,
			Description: "Database ID.",
			Computed:    true,
		},
		"reading_mode": {
			Type:        schema.TypeString,
			Description: "Reading mode.",
			Computed:    true,
		},
		"unexpected_type_display_mode": {
			Type:        schema.TypeString,
			Description: "Unexpected type display mode.",
			Computed:    true,
		},
		"unsupported_type_display_mode": {
			Type:        schema.TypeString,
			Description: "Unsupported type display mode.",
			Computed:    true,
		},
	}
}
