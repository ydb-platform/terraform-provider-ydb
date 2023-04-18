package coordination

import (
	"fmt"
	"net/url"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

type Resource struct {
	Entity                   *helpers.YDBEntity
	FullPath                 string
	Path                     string
	DatabaseEndpoint         string
	SelfCheckPeriodMillis    int
	SessionGracePeriodMillis int
	ReadConsistencyMode      coordination.ConsistencyMode
	AttachConsistencyMode    coordination.ConsistencyMode
	RatelimiterCountersMode  coordination.RatelimiterCountersMode
}

func ResourceToNodeConfig(resource *Resource) coordination.NodeConfig {
	return coordination.NodeConfig{
		Path:                     "",
		SelfCheckPeriodMillis:    uint32(resource.SelfCheckPeriodMillis),
		SessionGracePeriodMillis: uint32(resource.SessionGracePeriodMillis),
		ReadConsistencyMode:      resource.ReadConsistencyMode,
		AttachConsistencyMode:    resource.AttachConsistencyMode,
		RatelimiterCountersMode:  resource.RatelimiterCountersMode,
	}
}

const (
	ConsistencyModeRelaxed            = "relaxed"
	ConsistencyModeStrict             = "strict"
	ConsistencyModeUnset              = "unset"
	RatelimiterCountersModeAggregated = "aggregated"
	RatelimiterCountersModeDetailed   = "detailed"
	RatelimiterCountersModeUnset      = "unset"
)

func convertStringToConsistencyMode(s string) coordination.ConsistencyMode {
	if s == ConsistencyModeRelaxed {
		return coordination.ConsistencyModeRelaxed
	}
	if s == ConsistencyModeStrict {
		return coordination.ConsistencyModeStrict
	}
	return coordination.ConsistencyModeUnset
}

func convertConsistencyModeToString(c coordination.ConsistencyMode) string {
	if c == coordination.ConsistencyModeRelaxed {
		return ConsistencyModeRelaxed
	}
	if c == coordination.ConsistencyModeStrict {
		return ConsistencyModeStrict
	}
	return ConsistencyModeUnset
}

func convertStringToRatelimiterMode(s string) coordination.RatelimiterCountersMode {
	if s == RatelimiterCountersModeAggregated {
		return coordination.RatelimiterCountersModeAggregated
	}
	if s == RatelimiterCountersModeDetailed {
		return coordination.RatelimiterCountersModeDetailed
	}
	return coordination.RatelimiterCountersModeUnset
}

func convertRatelimiterModeToString(c coordination.RatelimiterCountersMode) string {
	if c == coordination.RatelimiterCountersModeAggregated {
		return RatelimiterCountersModeAggregated
	}
	if c == coordination.RatelimiterCountersModeDetailed {
		return RatelimiterCountersModeDetailed
	}
	return RatelimiterCountersModeUnset
}

func flattenCoordinationDescription(d *schema.ResourceData, desc *coordination.NodeConfig, entity *helpers.YDBEntity) (err error) {
	err = d.Set("path", entity.GetEntityPath())
	if err != nil {
		return
	}
	err = d.Set("connection_string", entity.PrepareFullYDBEndpoint())
	if err != nil {
		return
	}
	err = d.Set("self_check_period_ms", desc.SelfCheckPeriodMillis)
	if err != nil {
		return
	}
	err = d.Set("session_grace_period_ms", desc.SessionGracePeriodMillis)
	if err != nil {
		return
	}
	err = d.Set("read_consistency_mode", convertConsistencyModeToString(desc.ReadConsistencyMode))
	if err != nil {
		return
	}
	err = d.Set("attach_consistency_mode", convertConsistencyModeToString(desc.AttachConsistencyMode))
	if err != nil {
		return
	}
	err = d.Set("ratelimiter_counters_mode", convertRatelimiterModeToString(desc.RatelimiterCountersMode))
	if err != nil {
		return
	}
	return err
}

func ResourceSchemaToCoordinationResource(d *schema.ResourceData) (*Resource, error) {
	var entity *helpers.YDBEntity
	var err error
	if d.Id() != "" {
		entity, err = helpers.ParseYDBEntityID(d.Id())
		if err != nil {
			return nil, fmt.Errorf("failed to parse table entity: %w", err)
		}
	}

	databaseEndpoint := d.Get("connection_string").(string)
	databaseURL, err := url.Parse(databaseEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database endpoint: %w", err)
	}

	var path string
	if entity != nil {
		path = entity.GetEntityPath()
		databaseEndpoint = entity.PrepareFullYDBEndpoint()
		path = databaseEndpoint + "/" + path
	} else {
		path = databaseURL.Query().Get("database") + "/" + d.Get("path").(string)
		databaseEndpoint = d.Get("connection_string").(string)
	}
	return &Resource{
		Entity:                   entity,
		FullPath:                 path,
		Path:                     d.Get("path").(string),
		DatabaseEndpoint:         databaseEndpoint,
		SelfCheckPeriodMillis:    d.Get("self_check_period_ms").(int),
		SessionGracePeriodMillis: d.Get("session_grace_period_ms").(int),
		ReadConsistencyMode:      convertStringToConsistencyMode(d.Get("read_consistency_mode").(string)),
		AttachConsistencyMode:    convertStringToConsistencyMode(d.Get("attach_consistency_mode").(string)),
		RatelimiterCountersMode:  convertStringToRatelimiterMode(d.Get("ratelimiter_counters_mode").(string)),
	}, nil
}
