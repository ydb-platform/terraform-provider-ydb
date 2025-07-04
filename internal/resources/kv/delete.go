package kv

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/internal/kv"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func (h *handler) Delete(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
	kvResource, err := kvResourceSchemaToKvResource(d)
	if err != nil {
		return diag.FromErr(err)
	}
	if kvResource == nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "got nil resource, unreachable code",
			},
		}
	}

	conn, err := kv.CreateDBConnection(ctx, kv.ClientParams{
		DatabaseEndpoint: kvResource.Endpoint,
		UseTLS:           kvResource.UseTLS,
	})
	if err != nil {
		return diag.Errorf("failed to initialize kv client: %s", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	token, err := helpers.GetToken(ctx, h.authCreds, conn)
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "failed to get token",
				Detail:   err.Error(),
			},
		}
	}

	ctx, stub := kv.AddMetaDataKvStub(ctx, kv.ClientParams{
		Database: kvResource.Database,
		AuthCreds: auth.YdbCredentials{
			Token: token,
		},
	}, conn)

	return diag.FromErr(DropKvVolume(ctx, kvResource, stub))
}
