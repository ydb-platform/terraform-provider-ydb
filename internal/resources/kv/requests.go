package kv

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-genproto/draft/Ydb_KeyValue_V1"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_KeyValue"
)

func CreateKvVolume(ctx context.Context, kvResource *Resource, stub Ydb_KeyValue_V1.KeyValueServiceClient) error {

var channelMedia []*Ydb_KeyValue.StorageConfig_ChannelConfig
for _, v := range kvResource.StorageConfig.Channel {
	channelMedia = append(channelMedia, &Ydb_KeyValue.StorageConfig_ChannelConfig{Media: v.Media})
}

request := &Ydb_KeyValue.CreateVolumeRequest{
	Path:           kvResource.FullPath,
	PartitionCount: uint32(kvResource.PartitionCount),
	StorageConfig: &Ydb_KeyValue.StorageConfig{
		Channel: channelMedia,
	},
}

opResp, err := stub.CreateVolume(ctx, request)
if err != nil {
	return fmt.Errorf("create_volume problem: %v", err)
}
if opResp.Operation.Status.String() != "SUCCESS" {
	return fmt.Errorf("create operation code not success: %s, %v", opResp.Operation.Status.String(), opResp.Operation.Issues)
}

return nil

}

func DescribeKvVolume(ctx context.Context, kvResource *Resource, stub Ydb_KeyValue_V1.KeyValueServiceClient) (*Ydb_KeyValue.DescribeVolumeResult, error) {
	request := &Ydb_KeyValue.DescribeVolumeRequest{
		Path: kvResource.Entity.GetFullEntityPath(),
	}

	result := &Ydb_KeyValue.DescribeVolumeResult{}

	opResp, err := stub.DescribeVolume(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("describe_volume problem: %v", err)
	}

	if opResp.Operation.Status.String() == "SUCCESS" {
		err = opResp.Operation.Result.UnmarshalTo(result)
		if err != nil {
			return nil, fmt.Errorf("unmarshal_to problem: %v", err)
		}
	} else {
		return nil, fmt.Errorf("describe operation code not success: %s, %v", opResp.Operation.Status.String(), opResp.Operation.Issues)
	}
	return result, nil
}

func AlterKvVolume(ctx context.Context, d *schema.ResourceData, kvResource *Resource, stub Ydb_KeyValue_V1.KeyValueServiceClient) error {
	request := &Ydb_KeyValue.AlterVolumeRequest{}
	var old, new interface{}

	if d.HasChange("storage_config") {
		old, new = d.GetChange("storage_config")
		var channelMedia []*Ydb_KeyValue.StorageConfig_ChannelConfig
		for _, v := range kvResource.StorageConfig.Channel {
			channelMedia = append(channelMedia, &Ydb_KeyValue.StorageConfig_ChannelConfig{Media: v.Media})
		}
	
		request.Path = kvResource.Entity.GetFullEntityPath()
		request.AlterPartitionCount = uint32(kvResource.PartitionCount)
		request.StorageConfig = &Ydb_KeyValue.StorageConfig{Channel: channelMedia}
	} else {
		request.Path = kvResource.Entity.GetFullEntityPath()
		request.AlterPartitionCount = uint32(kvResource.PartitionCount)
	}

	opResp, err := stub.AlterVolume(ctx, request)
	if err != nil {
		return fmt.Errorf("alter_volume problem: %v", err)
	}

	if opResp.Operation.Status.String() != "SUCCESS" {
		if d.HasChange("storage_config") {
			err = d.Set("storage_config", old)
			if err != nil {
				return fmt.Errorf("can't set storage_config attrs: %v", err)
			}
		}
		return fmt.Errorf("alter operation code not success: %s, %v", opResp.Operation.Status.String(), opResp.Operation.Issues)
	}
	if d.HasChange("storage_config") {
		err = d.Set("storage_config", new)
		if err != nil {
			return fmt.Errorf("can't set storage_config attrs: %v", err)
		}
	}
	return nil
}

func DropKvVolume(ctx context.Context, kvResource *Resource, stub Ydb_KeyValue_V1.KeyValueServiceClient) error {
	request := &Ydb_KeyValue.DropVolumeRequest{
		Path: kvResource.Entity.GetFullEntityPath(),
	}

	opResp, err := stub.DropVolume(ctx, request)
	if err != nil {
		return fmt.Errorf("drop_volume problem: %v", err)
	}

	if opResp.Operation.Status.String() != "SUCCESS" {
		return fmt.Errorf("drop operation code not success: %s, %v", opResp.Operation.Status.String(), opResp.Operation.Issues)
	}
	return nil
}
