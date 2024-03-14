package kv

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/sourcecd/ydb-go-genproto/draft/protos/Ydb_KeyValue"
	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

type MediaConfig struct {
	Media string
}

type ChannelConfig struct {
	Channel []*MediaConfig
}

type Resource struct {
	Entity *helpers.YDBEntity

	FullPath             string
	Path                 string
	DatabaseEndpoint     string
	PartitionCount       int
	StorageConfig        *ChannelConfig
}

func kvResourceSchemaToKvResource(d *schema.ResourceData) (*Resource, error) {
	var entity *helpers.YDBEntity
	var err error
	if d.Id() != "" {
		entity, err = helpers.ParseYDBEntityID(d.Id())
		if err != nil {
			return nil, fmt.Errorf("failed to parse table entity: %w", err)
		}
	}

	storageConfig, err := expandStorageConfig(d)
	if err != nil {
		return nil, errors.New("can't expandStorageConfig")
	}

	partitionCount, ok := d.Get("partition_count").(int)
	if !ok {
		return nil, errors.New("can't parse partition_count")
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
		Entity:           entity,
		FullPath:         path,
		Path:             helpers.TrimPath(d.Get("path").(string)),
		DatabaseEndpoint: databaseEndpoint,
		PartitionCount:   partitionCount,
		StorageConfig:    storageConfig,
	}, nil
}

func flattenKvVolumeDescription(d *schema.ResourceData, desc *Ydb_KeyValue.DescribeVolumeResult, entity *helpers.YDBEntity) (err error) {
	err = d.Set("path", entity.GetEntityPath())
	if err != nil {
		return
	}
	err = d.Set("connection_string", entity.PrepareFullYDBEndpoint())
	if err != nil {
		return
	}

	err = d.Set("partition_count", desc.PartitionCount)
	if err != nil {
		return
	}

	return err
}
