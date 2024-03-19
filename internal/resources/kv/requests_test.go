package kv

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_KeyValue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	kv_mock "github.com/ydb-platform/terraform-provider-ydb/internal/resources/kv/mocks"
)

type mockBehaviorCreate func(
	mockClient *kv_mock.MockKeyValueServiceClient,
	req *Ydb_KeyValue.CreateVolumeRequest,
	expectedResponse *Ydb_KeyValue.CreateVolumeResponse,
)
type mockBehaviorDescribe func(
	mockClient *kv_mock.MockKeyValueServiceClient,
	req *Ydb_KeyValue.DescribeVolumeRequest,
	expectedResponse *Ydb_KeyValue.DescribeVolumeResponse,
)
type mockBehaviorAlter func(
	mockClient *kv_mock.MockKeyValueServiceClient,
	req *Ydb_KeyValue.AlterVolumeRequest,
	expectedResponse *Ydb_KeyValue.AlterVolumeResponse,
)
type mockBehaviorDrop func(
	mockClient *kv_mock.MockKeyValueServiceClient,
	req *Ydb_KeyValue.DropVolumeRequest,
	expectedResponse *Ydb_KeyValue.DropVolumeResponse,
)

func TestCreateKvVolume(t *testing.T) {
	createVolume := &Ydb_KeyValue.CreateVolumeRequest{
		Path:           "/Root/testpath",
		PartitionCount: 100,
		StorageConfig: &Ydb_KeyValue.StorageConfig{
			Channel: []*Ydb_KeyValue.StorageConfig_ChannelConfig{
				{
					Media: "ssd",
				},
				{
					Media: "ssd",
				},
				{
					Media: "ssd",
				},
			},
		},
	}

	testTable := []struct {
		name             string
		expectedRequest  *Ydb_KeyValue.CreateVolumeRequest
		expectedResponse *Ydb_KeyValue.CreateVolumeResponse
		mockBehavior     mockBehaviorCreate
		expectedError    error
	}{
		{
			name:            "OK",
			expectedRequest: createVolume,
			expectedResponse: &Ydb_KeyValue.CreateVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_SUCCESS,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.CreateVolumeRequest, expectedResponse *Ydb_KeyValue.CreateVolumeResponse) {
				mockClient.EXPECT().CreateVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
			expectedError: nil,
		},
		{
			name:            "ERROR CREATE",
			expectedRequest: createVolume,
			expectedResponse: &Ydb_KeyValue.CreateVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_STATUS_CODE_UNSPECIFIED,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.CreateVolumeRequest, expectedResponse *Ydb_KeyValue.CreateVolumeResponse) {
				mockClient.EXPECT().CreateVolume(gomock.Any(), req).Return(expectedResponse, fmt.Errorf("INTERNAL"))
			},
			expectedError: fmt.Errorf("create_volume problem: %w", fmt.Errorf("INTERNAL")),
		},
		{
			name:            "ERROR CODE",
			expectedRequest: createVolume,
			expectedResponse: &Ydb_KeyValue.CreateVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_INTERNAL_ERROR,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.CreateVolumeRequest, expectedResponse *Ydb_KeyValue.CreateVolumeResponse) {
				mockClient.EXPECT().CreateVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
			expectedError: fmt.Errorf("create operation code not success: INTERNAL_ERROR, []"),
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient := kv_mock.NewMockKeyValueServiceClient(ctrl)
			testCase.mockBehavior(mockClient, testCase.expectedRequest, testCase.expectedResponse)

			err := CreateKvVolume(ctx, &Resource{
				FullPath:       "/Root/testpath",
				PartitionCount: 100,
				StorageConfig: &ChannelConfig{
					Channel: []*MediaConfig{
						{
							Media: "ssd",
						},
						{
							Media: "ssd",
						},
						{
							Media: "ssd",
						},
					},
				},
			}, mockClient)
			assert.Equal(t, testCase.expectedError, err)
		})
	}
}

func TestDescribeKvVolume(t *testing.T) {
	entity, _ := helpers.ParseYDBEntityID("grpcs://test.local:2135/?database=/Root/testdb1?path=testdir/testpath")
	anyPb, err := anypb.New(&Ydb_KeyValue.DescribeVolumeResult{
		Path:           entity.GetFullEntityPath(),
		PartitionCount: 100,
	})
	if err != nil {
		t.Error(err)
	}
	anyPbFake, err := anypb.New(&Ydb_KeyValue.DescribeVolumeResponse{})
	if err != nil {
		t.Error(err)
	}
	expresult := &Ydb_KeyValue.DescribeVolumeResult{
		Path:           entity.GetFullEntityPath(),
		PartitionCount: 100,
	}
	testTable := []struct {
		name             string
		expectedRequest  *Ydb_KeyValue.DescribeVolumeRequest
		expectedResponse *Ydb_KeyValue.DescribeVolumeResponse
		mockBehavior     mockBehaviorDescribe
		expectedError    error
		expectedResult   *Ydb_KeyValue.DescribeVolumeResult
	}{
		{
			name: "OK",
			expectedRequest: &Ydb_KeyValue.DescribeVolumeRequest{
				Path: entity.GetFullEntityPath(),
			},
			expectedResponse: &Ydb_KeyValue.DescribeVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_SUCCESS,
					Result: anyPb,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.DescribeVolumeRequest, expectedResponse *Ydb_KeyValue.DescribeVolumeResponse) {
				mockClient.EXPECT().DescribeVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
			expectedResult: expresult,
			expectedError:  nil,
		},
		{
			name: "ERROR DESCRIBE",
			expectedRequest: &Ydb_KeyValue.DescribeVolumeRequest{
				Path: entity.GetFullEntityPath(),
			},
			expectedResponse: &Ydb_KeyValue.DescribeVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_SUCCESS,
					Result: anyPb,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.DescribeVolumeRequest, expectedResponse *Ydb_KeyValue.DescribeVolumeResponse) {
				mockClient.EXPECT().DescribeVolume(gomock.Any(), req).Return(expectedResponse, fmt.Errorf("describe error"))
			},
			expectedResult: nil,
			expectedError:  fmt.Errorf("describe_volume problem: %w", fmt.Errorf("describe error")),
		},
		{
			name: "ERROR DESCRIBE UNMARSHAL",
			expectedRequest: &Ydb_KeyValue.DescribeVolumeRequest{
				Path: entity.GetFullEntityPath(),
			},
			expectedResponse: &Ydb_KeyValue.DescribeVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_SUCCESS,
					Result: anyPbFake,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.DescribeVolumeRequest, expectedResponse *Ydb_KeyValue.DescribeVolumeResponse) {
				mockClient.EXPECT().DescribeVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
			expectedResult: nil,
			expectedError:  fmt.Errorf("unmarshal_to problem: %w", anyPbFake.UnmarshalTo(&Ydb_KeyValue.DescribeVolumeResult{})),
		},
		{
			name: "ERROR DESCRIBE STATUS",
			expectedRequest: &Ydb_KeyValue.DescribeVolumeRequest{
				Path: entity.GetFullEntityPath(),
			},
			expectedResponse: &Ydb_KeyValue.DescribeVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_PRECONDITION_FAILED,
					Result: anyPb,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.DescribeVolumeRequest, expectedResponse *Ydb_KeyValue.DescribeVolumeResponse) {
				mockClient.EXPECT().DescribeVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
			expectedResult: nil,
			expectedError:  fmt.Errorf("describe operation code not success: PRECONDITION_FAILED, []"),
		},
	}

	tuneResource := &Resource{
		Entity: entity,
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient := kv_mock.NewMockKeyValueServiceClient(ctrl)
			testCase.mockBehavior(mockClient, testCase.expectedRequest, testCase.expectedResponse)

			result, err := DescribeKvVolume(ctx, tuneResource, mockClient)

			assert.Equal(t, testCase.expectedError, err)
			assert.Equal(t, testCase.expectedResult.String(), result.String())
		})
	}
}

func TestAlterKvVolume(t *testing.T) {
	entity, _ := helpers.ParseYDBEntityID("grpcs://test.local:2135/?database=/Root/testdb1?path=testdir/testpath")
	requestWithStorage := &Ydb_KeyValue.AlterVolumeRequest{
		Path:                entity.GetFullEntityPath(),
		AlterPartitionCount: 101,
		StorageConfig: &Ydb_KeyValue.StorageConfig{
			Channel: []*Ydb_KeyValue.StorageConfig_ChannelConfig{
				{
					Media: "ssd",
				},
				{
					Media: "ssd",
				},
				{
					Media: "ssd",
				},
			},
		},
	}
	requestWithoutStorage := &Ydb_KeyValue.AlterVolumeRequest{
		Path:                entity.GetFullEntityPath(),
		AlterPartitionCount: 101,
	}

	testSchema := map[string]*schema.Schema{
		"storage_config": {
			Type: schema.TypeList,
			Elem: &schema.Resource{
				Schema: map[string]*schema.Schema{
					"channel": {
						Type: schema.TypeList,
						Elem: &schema.Resource{
							Schema: map[string]*schema.Schema{
								"media": {
									Type: schema.TypeString,
								},
							},
						},
					},
				},
			},
		},
	}
	testData := map[string]interface{}{
		"storage_config": []interface{}{
			map[string]interface{}{
				"channel": []interface{}{
					map[string]interface{}{
						"media": "ssd",
					},
					map[string]interface{}{
						"media": "ssd",
					},
					map[string]interface{}{
						"media": "ssd",
					},
				},
			},
		},
	}

	r := schema.Resource{
		Schema: testSchema,
	}
	d := schema.TestResourceDataRaw(t, r.Schema, testData)
	d.SetId("test")

	// saved state
	dn := r.Data(d.State())

	testTable := []struct {
		name             string
		expectedRequest  *Ydb_KeyValue.AlterVolumeRequest
		expectedResponse *Ydb_KeyValue.AlterVolumeResponse
		mockBehavior     mockBehaviorAlter
		expectedError    error
		schema           *schema.ResourceData
	}{
		{
			name:            "OK",
			expectedRequest: requestWithStorage,
			expectedResponse: &Ydb_KeyValue.AlterVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_SUCCESS,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.AlterVolumeRequest, expectedResponse *Ydb_KeyValue.AlterVolumeResponse) {
				mockClient.EXPECT().AlterVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
			expectedError: nil,
			schema:        d,
		},
		{
			name:            "HAS STORAGE CHANGES",
			expectedRequest: requestWithoutStorage,
			expectedResponse: &Ydb_KeyValue.AlterVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_SUCCESS,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.AlterVolumeRequest, expectedResponse *Ydb_KeyValue.AlterVolumeResponse) {
				mockClient.EXPECT().AlterVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
			expectedError: nil,
			schema:        dn,
		},
		{
			name:            "HAS ALTER ERROR",
			expectedRequest: requestWithoutStorage,
			expectedResponse: &Ydb_KeyValue.AlterVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_SUCCESS,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.AlterVolumeRequest, expectedResponse *Ydb_KeyValue.AlterVolumeResponse) {
				mockClient.EXPECT().AlterVolume(gomock.Any(), req).Return(expectedResponse, fmt.Errorf("alter error"))
			},
			expectedError: fmt.Errorf("alter_volume problem: %w", fmt.Errorf("alter error")),
			schema:        dn,
		},
		{
			name:            "NOT SUCCESS CODE",
			expectedRequest: requestWithoutStorage,
			expectedResponse: &Ydb_KeyValue.AlterVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_ABORTED,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.AlterVolumeRequest, expectedResponse *Ydb_KeyValue.AlterVolumeResponse) {
				mockClient.EXPECT().AlterVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
			expectedError: fmt.Errorf("alter operation code not success: ABORTED, []"),
			schema:        dn,
		},
		{
			name:            "NOT SUCCESS CODE WITH CHANGE",
			expectedRequest: requestWithStorage,
			expectedResponse: &Ydb_KeyValue.AlterVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_ABORTED,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.AlterVolumeRequest, expectedResponse *Ydb_KeyValue.AlterVolumeResponse) {
				mockClient.EXPECT().AlterVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
			expectedError: fmt.Errorf("alter operation code not success: ABORTED, []"),
			schema:        d,
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient := kv_mock.NewMockKeyValueServiceClient(ctrl)
			testCase.mockBehavior(mockClient, testCase.expectedRequest, testCase.expectedResponse)

			err := AlterKvVolume(ctx, testCase.schema, &Resource{
				Entity:         entity,
				PartitionCount: 101,
				StorageConfig: &ChannelConfig{
					Channel: []*MediaConfig{
						{
							Media: "ssd",
						},
						{
							Media: "ssd",
						},
						{
							Media: "ssd",
						},
					},
				},
			}, mockClient)

			assert.Equal(t, testCase.expectedError, err)
		})
	}
}

func TestDropKvVolume(t *testing.T) {
	entity, _ := helpers.ParseYDBEntityID("grpcs://test.local:2135/?database=/Root/testdb1?path=testdir/testpath")
	testTable := []struct {
		name             string
		expectedRequest  *Ydb_KeyValue.DropVolumeRequest
		expectedResponse *Ydb_KeyValue.DropVolumeResponse
		mockBehavior     mockBehaviorDrop
		expectedError    error
	}{
		{
			name: "OK",
			expectedRequest: &Ydb_KeyValue.DropVolumeRequest{
				Path: entity.GetFullEntityPath(),
			},
			expectedResponse: &Ydb_KeyValue.DropVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_SUCCESS,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.DropVolumeRequest, expectedResponse *Ydb_KeyValue.DropVolumeResponse) {
				mockClient.EXPECT().DropVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
			expectedError: nil,
		},
		{
			name: "ERROR DROP",
			expectedRequest: &Ydb_KeyValue.DropVolumeRequest{
				Path: entity.GetFullEntityPath(),
			},
			expectedResponse: &Ydb_KeyValue.DropVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_SUCCESS,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.DropVolumeRequest, expectedResponse *Ydb_KeyValue.DropVolumeResponse) {
				mockClient.EXPECT().DropVolume(gomock.Any(), req).Return(expectedResponse, fmt.Errorf("error drop"))
			},
			expectedError: fmt.Errorf("drop_volume problem: %w", fmt.Errorf("error drop")),
		},
		{
			name: "ERROR CODE",
			expectedRequest: &Ydb_KeyValue.DropVolumeRequest{
				Path: entity.GetFullEntityPath(),
			},
			expectedResponse: &Ydb_KeyValue.DropVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_PRECONDITION_FAILED,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.DropVolumeRequest, expectedResponse *Ydb_KeyValue.DropVolumeResponse) {
				mockClient.EXPECT().DropVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
			expectedError: fmt.Errorf("drop operation code not success: PRECONDITION_FAILED, []"),
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient := kv_mock.NewMockKeyValueServiceClient(ctrl)
			testCase.mockBehavior(mockClient, testCase.expectedRequest, testCase.expectedResponse)

			err := DropKvVolume(ctx, &Resource{
				Entity: entity,
			}, mockClient)

			assert.Equal(t, testCase.expectedError, err)
		})
	}
}
