package kv

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/internal/kv"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func (h *handler) Update(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
	kvResource, err := kvResourceSchemaToKvResource(d)
	if err != nil {
		return diag.FromErr(err)
	}

	conn, err := kv.CreateDBConnection(ctx, kv.ClientParams{
		DatabaseEndpoint: kvResource.Endpoint,
		UseTLS:           kvResource.UseTLS,
	})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "failed to initialize kv client",
				Detail:   err.Error(),
			},
		}
	}
	defer func() {
		_ = conn.Close()
	}()

	token, err := helpers.GetToken(ctx, h.authCreds, conn)
	if err != nil {
		return diag.Diagnostics{
			{
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

	err = AlterKvVolume(ctx, d, kvResource, stub)
	if err != nil {
		return diag.FromErr(err)
	}

	return h.Read(ctx, d, cfg)
}
