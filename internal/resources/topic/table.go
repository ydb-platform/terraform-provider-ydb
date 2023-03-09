package topic

import (
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

type Resource struct {
	Entity *helpers.YDBEntity

	FullPath         string
	Path             string
	DatabaseEndpoint string
}

func tableResourceSchemaToTableResource(d *schema.ResourceData) (*Resource, error) {
	var entity *helpers.YDBEntity
	var err error
	if d.Id() != "" {
		entity, err = helpers.ParseYDBEntityID(d.Id())
		if err != nil {
			return nil, fmt.Errorf("failed to parse table entity: %w", err)
		}
	}

	var databaseEndpoint string
	if entity != nil {
		databaseEndpoint = entity.PrepareFullYDBEndpoint()
	} else {
		// NOTE(shmel1k@): resource is not initialized yet.
		databaseEndpoint = d.Get("database_endpoint").(string)
	}

	return &Resource{
		Entity:           entity,
		DatabaseEndpoint: databaseEndpoint,
	}, nil
}
