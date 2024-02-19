package coordination

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

func coordinationDiff(d *schema.ResourceData) coordination.NodeConfig {
	var diff coordination.NodeConfig
	if d.HasChange("self_check_period_ms") {
		v, _ := d.GetOk("self_check_period_ms")
		diff.SelfCheckPeriodMillis = uint32(v.(int))
	} else {
		diff.SessionGracePeriodMillis = uint32(d.Get("self_check_period_ms").(int))
	}
	if d.HasChange("session_grace_period_ms") {
		v, _ := d.GetOk("session_grace_period_ms")
		diff.SessionGracePeriodMillis = uint32(v.(int))
	} else {
		diff.SessionGracePeriodMillis = uint32(d.Get("session_grace_period_ms").(int))
	}
	if d.HasChange("read_consistency_mode") {
		v, _ := d.GetOk("read_consistency_mode")
		diff.ReadConsistencyMode = convertStringToConsistencyMode(v.(string))
	} else {
		diff.ReadConsistencyMode = convertStringToConsistencyMode(d.Get("read_consistency_mode").(string))
	}
	if d.HasChange("attach_consistency_mode") {
		v, _ := d.GetOk("attach_consistency_mode")
		diff.AttachConsistencyMode = convertStringToConsistencyMode(v.(string))
	} else {
		diff.AttachConsistencyMode = convertStringToConsistencyMode(d.Get("attach_consistency_mode").(string))
	}
	if d.HasChange("ratelimiter_counters_mode") {
		v, _ := d.GetOk("ratelimiter_counters_mode")
		diff.RatelimiterCountersMode = convertStringToRatelimiterMode(v.(string))
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
		AuthCreds:        h.authCreds,
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
