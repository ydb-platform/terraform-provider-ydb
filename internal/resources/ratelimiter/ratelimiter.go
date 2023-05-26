package ratelimiter

import (
	"fmt"
	"net/url"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
)

type Resource struct {
	Entity                  *helpers.YDBEntity
	FullPath                string
	Path                    string
	DatabaseEndpoint        string
	ResourcePath            string
	MaxUnitsPerSecond       float64
	MaxBurstSizeCoefficient float64
	PrefetchCoefficient     float64
	PrefetchWatermark       float64
}

func ResourceToRateLimiterResource(resource *Resource) ratelimiter.Resource {
	return ratelimiter.Resource{
		ResourcePath: resource.ResourcePath,
		HierarchicalDrr: ratelimiter.HierarchicalDrrSettings{
			MaxUnitsPerSecond:       resource.MaxUnitsPerSecond,
			MaxBurstSizeCoefficient: resource.MaxBurstSizeCoefficient,
			PrefetchCoefficient:     resource.PrefetchCoefficient,
			PrefetchWatermark:       resource.PrefetchWatermark,
		},
	}
}

func flattenRateLimiterDescription(d *schema.ResourceData, desc *ratelimiter.Resource, entity *helpers.YDBEntity) (err error) {
	err = d.Set("path", entity.GetEntityPath())
	if err != nil {
		return
	}
	err = d.Set("connection_string", entity.PrepareFullYDBEndpoint())
	if err != nil {
		return
	}
	err = d.Set("resource_path", desc.ResourcePath)
	if err != nil {
		return
	}
	err = d.Set("max_units_per_second", desc.HierarchicalDrr.MaxUnitsPerSecond)
	if err != nil {
		return
	}
	err = d.Set("max_burst_size_coefficient", desc.HierarchicalDrr.MaxBurstSizeCoefficient)
	if err != nil {
		return
	}
	err = d.Set("prefetch_coefficient", desc.HierarchicalDrr.PrefetchCoefficient)
	if err != nil {
		return
	}
	err = d.Set("prefetch_watermark", desc.HierarchicalDrr.PrefetchWatermark)
	return
}

func ResourceSchemaToRateLimiterResource(d *schema.ResourceData) (*Resource, error) {
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
		Entity:                  entity,
		FullPath:                path,
		Path:                    d.Get("path").(string),
		DatabaseEndpoint:        databaseEndpoint,
		ResourcePath:            d.Get("resource_path").(string),
		MaxUnitsPerSecond:       d.Get("max_units_per_second").(float64),
		MaxBurstSizeCoefficient: d.Get("max_burst_size_coefficient").(float64),
		PrefetchCoefficient:     d.Get("prefetch_coefficient").(float64),
		PrefetchWatermark:       d.Get("prefetch_watermark").(float64),
	}, nil
}
