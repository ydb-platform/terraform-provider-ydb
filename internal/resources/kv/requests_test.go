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

	kv_mock "github.com/ydb-platform/terraform-provider-ydb/internal/resources/kv/mocks"
)

type mockBehavior func(
	mockClient *kv_mock.MockKeyValueServiceClient,
	req *Ydb_KeyValue.CreateVolumeRequest,
	expectedResponse *Ydb_KeyValue.CreateVolumeResponse,
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
		mockBehavior     mockBehavior
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
