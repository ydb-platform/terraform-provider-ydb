package kv

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_KeyValue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"

	kv_mock "github.com/ydb-platform/terraform-provider-ydb/internal/resources/kv/mocks"
)

type mockBehavior func(
	mockClient *kv_mock.MockKeyValueServiceClient,
	req *Ydb_KeyValue.CreateVolumeRequest,
	expectedResponse *Ydb_KeyValue.CreateVolumeResponse,
)

func TestCreateKvVolume(t *testing.T) {
	testTable := []struct {
		name             string
		expectedRequest  *Ydb_KeyValue.CreateVolumeRequest
		expectedResponse *Ydb_KeyValue.CreateVolumeResponse
		mockBehavior     mockBehavior
	}{
		{
			name: "OK",
			expectedRequest: &Ydb_KeyValue.CreateVolumeRequest{
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
			},
			expectedResponse: &Ydb_KeyValue.CreateVolumeResponse{
				Operation: &Ydb_Operations.Operation{
					Status: Ydb.StatusIds_SUCCESS,
				},
			},
			mockBehavior: func(mockClient *kv_mock.MockKeyValueServiceClient, req *Ydb_KeyValue.CreateVolumeRequest, expectedResponse *Ydb_KeyValue.CreateVolumeResponse) {
				mockClient.EXPECT().CreateVolume(gomock.Any(), req).Return(expectedResponse, nil)
			},
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
			if err != nil {
				t.Error(err)
			}
		})
	}
}
