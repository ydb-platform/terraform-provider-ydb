package resourcepool

import (
	"context"
	"fmt"
	"math"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	resourcePoolHandler "github.com/ydb-platform/terraform-provider-ydb/internal/resources/resourcepool"
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
			Description: "Unique resource pool name. Path notation is not allowed, so the name must not contain '/'.",
			ValidateFunc: validation.All(
				validation.StringIsNotEmpty,
				validation.StringDoesNotContainAny("/"),
			),
		},
		"concurrent_query_limit": {
			Type:         schema.TypeInt,
			Optional:     true,
			Default:      -1,
			Description:  "Number of concurrently executing queries. Allowed values: -1 or 0 through 2^31 - 1. -1 disables the limit; 0 immediately rejects pool queries.",
			ValidateFunc: validation.IntBetween(-1, math.MaxInt32),
		},
		"queue_size": {
			Type:         schema.TypeInt,
			Optional:     true,
			Default:      -1,
			Description:  "Maximum number of queries in the waiting queue. Allowed values: -1 or 0 through 2^31 - 1. -1 disables the limit.",
			ValidateFunc: validation.IntBetween(-1, math.MaxInt32),
		},
		"database_load_cpu_threshold": {
			Type:         schema.TypeInt,
			Optional:     true,
			Default:      -1,
			Description:  "Database CPU load threshold after which queries remain queued. Allowed values: -1 or 0 through 100. -1 disables the threshold.",
			ValidateFunc: validation.IntBetween(-1, 100),
		},
		"resource_weight": {
			Type:         schema.TypeInt,
			Optional:     true,
			Default:      -1,
			Description:  "Weight used to distribute resources between pools. Allowed values: -1 or 0 through 100. -1 disables weighted distribution.",
			ValidateFunc: validation.IntBetween(-1, 100),
		},
		"total_cpu_limit_percent_per_node": {
			Type:         schema.TypeFloat,
			Optional:     true,
			Default:      -1,
			Description:  "Percentage of CPU on one node available to all pool queries. Allowed values: -1 or 0 through 100. -1 disables the limit.",
			ValidateFunc: validatePercent,
		},
		"query_cpu_limit_percent_per_node": {
			Type:         schema.TypeFloat,
			Optional:     true,
			Default:      -1,
			Description:  "Percentage of CPU on one node available to a single pool query. Allowed values: -1 or 0 through 100. -1 disables the limit.",
			ValidateFunc: validatePercent,
		},
		"total_memory_limit_percent_per_node": {
			Type:         schema.TypeFloat,
			Optional:     true,
			Default:      -1,
			Description:  "Percentage of memory on one node available to all pool queries. Allowed values: -1 or 0 through 100. -1 disables the limit.",
			ValidateFunc: validatePercent,
		},
	}
}

func validatePercent(value interface{}, key string) ([]string, []error) {
	percent := value.(float64)
	if percent == -1 || percent >= 0 && percent <= 100 {
		return nil, nil
	}
	return nil, []error{fmt.Errorf("%q must be -1 or between 0 and 100, got %v", key, percent)}
}

func ResourceCreateFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return authError(err)
		}
		h := resourcePoolHandler.NewHandler(authCreds)
		return h.Create(ctx, d, meta)
	}
}

func ResourceReadFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return authError(err)
		}
		h := resourcePoolHandler.NewHandler(authCreds)
		return h.Read(ctx, d, meta)
	}
}

func ResourceUpdateFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return authError(err)
		}
		h := resourcePoolHandler.NewHandler(authCreds)
		return h.Update(ctx, d, meta)
	}
}

func ResourceDeleteFunc(cb auth.GetAuthCallback) helpers.TerraformCRUD {
	return func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		authCreds, err := cb(ctx)
		if err != nil {
			return authError(err)
		}
		h := resourcePoolHandler.NewHandler(authCreds)
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
