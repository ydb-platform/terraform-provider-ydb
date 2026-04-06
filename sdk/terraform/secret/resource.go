package secret

import (
	"context"
	"encoding/base64"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"golang.org/x/crypto/scrypt"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	secretHandler "github.com/ydb-platform/terraform-provider-ydb/internal/resources/secret"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

// hashSecretValue hashes the secret using scrypt so the plaintext is not stored in Terraform state.
// See rationale for scrypt choice:
// https://github.com/yandex-cloud/terraform-provider-yandex/blob/master/yandex/resource_yandex_lockbox_secret_version_hashed.go#L121-L128
func hashSecretValue(v interface{}) string {
	value := v.(string)
	if value == "" {
		return ""
	}
	salt := []byte("|82&pvyYC[el3Z([,En#1:£!VJ2fKz")
	hash, err := scrypt.Key([]byte(value), salt, 32768, 8, 1, 128)
	if err != nil {
		log.Printf("[ERROR] could not hash secret value: %v", err)
		return ""
	}
	return base64.StdEncoding.EncodeToString(hash)
}

func ResourceSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		"connection_string": {
			Type:        schema.TypeString,
			Required:    true,
			ForceNew:    true,
			Description: "Connection string for YDB database.",
		},
		"name": {
			Type:        schema.TypeString,
			Required:    true,
			ForceNew:    true,
			Description: "Secret name.",
		},
		"value": {
			Type:        schema.TypeString,
			Required:    true,
			Sensitive:   true,
			StateFunc:   hashSecretValue,
			Description: "Secret value. This value is sensitive and will not be displayed in plan output.",
		},
		"inherit_permissions": {
			Type:        schema.TypeBool,
			Optional:    true,
			Default:     false,
			ForceNew:    true,
			Description: "If true, the secret inherits access rights from its parent directory. If false (default), only DESCRIBE SCHEMA permission is inherited.",
		},
	}
}

func DataSourceSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		"connection_string": {
			Type:        schema.TypeString,
			Required:    true,
			Description: "Connection string for YDB database.",
		},
		"name": {
			Type:        schema.TypeString,
			Required:    true,
			Description: "Secret name.",
		},
	}
}

func ResourceCreateFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{Severity: diag.Error, Summary: "failed to create token for YDB request", Detail: err.Error()},
			}
		}
		h := secretHandler.NewHandler(authCreds)
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
		h := secretHandler.NewHandler(authCreds)
		return h.Read(ctx, d, meta)
	}
}

func ResourceUpdateFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return diag.Diagnostics{
				{Severity: diag.Error, Summary: "failed to create token for YDB request", Detail: err.Error()},
			}
		}
		h := secretHandler.NewHandler(authCreds)
		return h.Update(ctx, d, meta)
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
		h := secretHandler.NewHandler(authCreds)
		return h.Delete(ctx, d, meta)
	}
}
