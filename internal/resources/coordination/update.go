package coordination

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
)

func coordinationDiff(d *schema.ResourceData) coordination.NodeConfig {
	var diff coordination.NodeConfig
	if d.HasChange("self_check_period_ms") {
		if v, ok := d.GetOk("self_check_period_ms"); ok {
			diff.SelfCheckPeriodMillis = uint32(v.(int))
		}
	} else {
		diff.SessionGracePeriodMillis = uint32(d.Get("self_check_period_ms").(int))
	}
	if d.HasChange("session_grace_period_ms") {
		if v, ok := d.GetOk("session_grace_period_ms"); ok {
			diff.SessionGracePeriodMillis = uint32(v.(int))
		}
	} else {
		diff.SessionGracePeriodMillis = uint32(d.Get("session_grace_period_ms").(int))
	}
	if d.HasChange("read_consistency_mode") {
		if v, ok := d.GetOk("read_consistency_mode"); ok {
			diff.ReadConsistencyMode = convertStringToConsistencyMode(v.(string))
		}
	} else {
		diff.ReadConsistencyMode = convertStringToConsistencyMode(d.Get("read_consistency_mode").(string))
	}
	if d.HasChange("attach_consistency_mode") {
		if v, ok := d.GetOk("attach_consistency_mode"); ok {
			diff.AttachConsistencyMode = convertStringToConsistencyMode(v.(string))
		}
	} else {
		diff.AttachConsistencyMode = convertStringToConsistencyMode(d.Get("attach_consistency_mode").(string))
	}
	if d.HasChange("ratelimiter_counters_mode") {
		if v, ok := d.GetOk("ratelimiter_counters_mode"); ok {
			diff.RatelimiterCountersMode = convertStringToRatelimiterMode(v.(string))
		}
	} else {
		diff.RatelimiterCountersMode = convertStringToRatelimiterMode(d.Get("ratelimiter_counters_mode").(string))
	}
	return diff
}

func (h handlerCoordination) Update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	coordinationResource, err := ResourceSchemaToCoordinationResource(d)
	if err != nil {
		return diag.FromErr(err)
	}
	if coordinationResource == nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "got nil resource, unreachable code",
			},
		}
	}
	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: coordinationResource.DatabaseEndpoint,
		Token:            h.token,
	})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "failed to initialize table client",
				Detail:   err.Error(),
			},
		}
	}
	defer func() {
		_ = db.Close(ctx)
	}()
	config := coordinationDiff(d)
	err = db.Coordination().AlterNode(ctx, coordinationResource.Path, config)
	if err != nil {
		return diag.FromErr(err)
	}
	return nil
}
