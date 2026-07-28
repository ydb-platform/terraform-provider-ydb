package resourcepoolclassifier

import (
	"context"
	"math"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	resourcePoolClassifierHandler "github.com/ydb-platform/terraform-provider-ydb/internal/resources/resourcepoolclassifier"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func ResourceSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		"connection_string": {
			Type:        schema.TypeString,
			Required:    true,
			ForceNew:    true,
			Description: "Connection string for the YDB database.",
		},
		"name": {
			Type:        schema.TypeString,
			Required:    true,
			ForceNew:    true,
			Description: "Unique resource pool classifier name.",
			ValidateFunc: validation.All(
				validation.StringIsNotEmpty,
				validation.StringDoesNotContainAny("/"),
			),
		},
		"rank": {
			Type:         schema.TypeInt,
			Optional:     true,
			Computed:     true,
			Description:  "Unique classifier evaluation order from 0 through 2^63 - 1. If omitted, YDB assigns the maximum existing rank plus 1000.",
			ValidateFunc: validation.IntBetween(0, math.MaxInt64),
		},
		"resource_pool": {
			Type:        schema.TypeString,
			Required:    true,
			Description: "Name of the resource pool to which matching queries are routed.",
			ValidateFunc: validation.All(
				validation.StringIsNotEmpty,
				validation.StringDoesNotContainAny("/"),
			),
		},
		"member_name": {
			Type:         schema.TypeString,
			Optional:     true,
			Description:  "User or group SID matched character by character against the authenticated user's SID and group SIDs. If omitted, this criterion is ignored.",
			ValidateFunc: validation.StringIsNotEmpty,
		},
	}
}

func ResourceCreateFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return authError(err)
		}
		h := resourcePoolClassifierHandler.NewHandler(authCreds)
		return h.Create(ctx, d, meta)
	}
}

func ResourceReadFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return authError(err)
		}
		h := resourcePoolClassifierHandler.NewHandler(authCreds)
		return h.Read(ctx, d, meta)
	}
}

func ResourceUpdateFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return authError(err)
		}
		h := resourcePoolClassifierHandler.NewHandler(authCreds)
		return h.Update(ctx, d, meta)
	}
}

func ResourceDeleteFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return authError(err)
		}
		h := resourcePoolClassifierHandler.NewHandler(authCreds)
		return h.Delete(ctx, d, meta)
	}
}

func authError(err error) diag.Diagnostics {
	return diag.Diagnostics{
		{
			Severity: diag.Error,
			Summary:  "failed to create token for YDB request",
			Detail:   err.Error(),
		},
	}
}
