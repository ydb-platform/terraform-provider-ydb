package kv

import (
	"fmt"
	"context"
	"github.com/sourcecd/ydb-go-genproto/draft/Ydb_KeyValue_V1"
	"github.com/sourcecd/ydb-go-genproto/draft/protos/Ydb_KeyValue"
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
	return fmt.Errorf("operation code not success: %s, %v", opResp.Operation.Status.String(), opResp.Operation.Issues)
}

return nil

}

func DescribeKvVolume(ctx context.Context, kvResource *Resource, stub Ydb_KeyValue_V1.KeyValueServiceClient) (*Ydb_KeyValue.DescribeVolumeResult, error) {
	request := &Ydb_KeyValue.DescribeVolumeRequest{
		Path: kvResource.FullPath,
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
		return nil, fmt.Errorf("%s, %v\n", opResp.Operation.Status, opResp.Operation.Issues)
	}
	return result, nil
}

func AlterKvVolume(ctx context.Context, kvResource *Resource, stub Ydb_KeyValue_V1.KeyValueServiceClient) error {
	request := &Ydb_KeyValue.AlterVolumeRequest{
		Path:                kvResource.FullPath,
		AlterPartitionCount: uint32(kvResource.PartitionCount),
	}

	opResp, err := stub.AlterVolume(ctx, request)
	if err != nil {
		return fmt.Errorf("alter_volume problem: %v", err)
	}

	if opResp.Operation.Status.String() != "SUCCESS" {
		return fmt.Errorf("operation code not success: %s, %v", opResp.Operation.Status.String(), opResp.Operation.Issues)
	}
	return nil
}

func DropKvVolume(ctx context.Context, kvResource *Resource, stub Ydb_KeyValue_V1.KeyValueServiceClient) error {
	request := &Ydb_KeyValue.DropVolumeRequest{
		Path: kvResource.FullPath,
	}

	opResp, err := stub.DropVolume(ctx, request)
	if err != nil {
		return fmt.Errorf("drop_volume problem: %v", err)
	}

	if opResp.Operation.Status.String() != "SUCCESS" {
		return fmt.Errorf("operation code not success: %s, %v", opResp.Operation.Status.String(), opResp.Operation.Issues)
	}
	return nil
}