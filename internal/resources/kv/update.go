package kv

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	"github.com/ydb-platform/terraform-provider-ydb/internal/kv"
)

func (h *handler) Update(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
	var token string
	kvResource, err := kvResourceSchemaToKvResource(d)
	if err != nil {
		return diag.FromErr(err)
	}

	conn, err := kv.CreateDBConnection(ctx, kv.ClientParams{
		DatabaseEndpoint: kvResource.Endpoint,
		UseTls:           kvResource.UseTls,
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

	if h.authCreds.User != "" {
		token, err = auth.GetTokenFromStaticCreds(ctx, h.authCreds.User, h.authCreds.Password, conn)
		if err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Error,
					Summary:  "failed to get auth token for static creds",
					Detail:   err.Error(),
				},
			}
		}
	} else {
		token = h.authCreds.Token
	}

	ctx, stub := kv.AddMetaDataKvStub(ctx, kv.ClientParams{
		Database: kvResource.Database,
		AuthCreds: auth.YdbCredentials{
			Token: token,
		},
	}, conn)

	err = AlterKvVolume(ctx, kvResource, stub)
	if err != nil {
		return diag.FromErr(err)
	}

	return h.Read(ctx, d, cfg)
}