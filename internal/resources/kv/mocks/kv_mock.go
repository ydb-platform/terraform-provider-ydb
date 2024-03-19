// Code generated by MockGen. DO NOT EDIT.
// Source: ../../../vendor/github.com/ydb-platform/ydb-go-genproto/draft/Ydb_KeyValue_V1/ydb_keyvalue_v1_grpc.pb.go

// Package mock_Ydb_KeyValue_V1 is a generated GoMock package.
package mock_Ydb_KeyValue_V1

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	Ydb_KeyValue "github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_KeyValue"
	grpc "google.golang.org/grpc"
	reflect "reflect"
)

// MockKeyValueServiceClient is a mock of KeyValueServiceClient interface
type MockKeyValueServiceClient struct {
	ctrl     *gomock.Controller
	recorder *MockKeyValueServiceClientMockRecorder
}

// MockKeyValueServiceClientMockRecorder is the mock recorder for MockKeyValueServiceClient
type MockKeyValueServiceClientMockRecorder struct {
	mock *MockKeyValueServiceClient
}

// NewMockKeyValueServiceClient creates a new mock instance
func NewMockKeyValueServiceClient(ctrl *gomock.Controller) *MockKeyValueServiceClient {
	mock := &MockKeyValueServiceClient{ctrl: ctrl}
	mock.recorder = &MockKeyValueServiceClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockKeyValueServiceClient) EXPECT() *MockKeyValueServiceClientMockRecorder {
	return m.recorder
}

// CreateVolume mocks base method
func (m *MockKeyValueServiceClient) CreateVolume(ctx context.Context, in *Ydb_KeyValue.CreateVolumeRequest, opts ...grpc.CallOption) (*Ydb_KeyValue.CreateVolumeResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreateVolume", varargs...)
	ret0, _ := ret[0].(*Ydb_KeyValue.CreateVolumeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateVolume indicates an expected call of CreateVolume
func (mr *MockKeyValueServiceClientMockRecorder) CreateVolume(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateVolume", reflect.TypeOf((*MockKeyValueServiceClient)(nil).CreateVolume), varargs...)
}

// DropVolume mocks base method
func (m *MockKeyValueServiceClient) DropVolume(ctx context.Context, in *Ydb_KeyValue.DropVolumeRequest, opts ...grpc.CallOption) (*Ydb_KeyValue.DropVolumeResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DropVolume", varargs...)
	ret0, _ := ret[0].(*Ydb_KeyValue.DropVolumeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DropVolume indicates an expected call of DropVolume
func (mr *MockKeyValueServiceClientMockRecorder) DropVolume(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DropVolume", reflect.TypeOf((*MockKeyValueServiceClient)(nil).DropVolume), varargs...)
}

// AlterVolume mocks base method
func (m *MockKeyValueServiceClient) AlterVolume(ctx context.Context, in *Ydb_KeyValue.AlterVolumeRequest, opts ...grpc.CallOption) (*Ydb_KeyValue.AlterVolumeResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AlterVolume", varargs...)
	ret0, _ := ret[0].(*Ydb_KeyValue.AlterVolumeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AlterVolume indicates an expected call of AlterVolume
func (mr *MockKeyValueServiceClientMockRecorder) AlterVolume(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AlterVolume", reflect.TypeOf((*MockKeyValueServiceClient)(nil).AlterVolume), varargs...)
}

// DescribeVolume mocks base method
func (m *MockKeyValueServiceClient) DescribeVolume(ctx context.Context, in *Ydb_KeyValue.DescribeVolumeRequest, opts ...grpc.CallOption) (*Ydb_KeyValue.DescribeVolumeResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DescribeVolume", varargs...)
	ret0, _ := ret[0].(*Ydb_KeyValue.DescribeVolumeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeVolume indicates an expected call of DescribeVolume
func (mr *MockKeyValueServiceClientMockRecorder) DescribeVolume(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeVolume", reflect.TypeOf((*MockKeyValueServiceClient)(nil).DescribeVolume), varargs...)
}

// ListLocalPartitions mocks base method
func (m *MockKeyValueServiceClient) ListLocalPartitions(ctx context.Context, in *Ydb_KeyValue.ListLocalPartitionsRequest, opts ...grpc.CallOption) (*Ydb_KeyValue.ListLocalPartitionsResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListLocalPartitions", varargs...)
	ret0, _ := ret[0].(*Ydb_KeyValue.ListLocalPartitionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListLocalPartitions indicates an expected call of ListLocalPartitions
func (mr *MockKeyValueServiceClientMockRecorder) ListLocalPartitions(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListLocalPartitions", reflect.TypeOf((*MockKeyValueServiceClient)(nil).ListLocalPartitions), varargs...)
}

// AcquireLock mocks base method
func (m *MockKeyValueServiceClient) AcquireLock(ctx context.Context, in *Ydb_KeyValue.AcquireLockRequest, opts ...grpc.CallOption) (*Ydb_KeyValue.AcquireLockResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AcquireLock", varargs...)
	ret0, _ := ret[0].(*Ydb_KeyValue.AcquireLockResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AcquireLock indicates an expected call of AcquireLock
func (mr *MockKeyValueServiceClientMockRecorder) AcquireLock(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AcquireLock", reflect.TypeOf((*MockKeyValueServiceClient)(nil).AcquireLock), varargs...)
}

// ExecuteTransaction mocks base method
func (m *MockKeyValueServiceClient) ExecuteTransaction(ctx context.Context, in *Ydb_KeyValue.ExecuteTransactionRequest, opts ...grpc.CallOption) (*Ydb_KeyValue.ExecuteTransactionResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ExecuteTransaction", varargs...)
	ret0, _ := ret[0].(*Ydb_KeyValue.ExecuteTransactionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecuteTransaction indicates an expected call of ExecuteTransaction
func (mr *MockKeyValueServiceClientMockRecorder) ExecuteTransaction(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteTransaction", reflect.TypeOf((*MockKeyValueServiceClient)(nil).ExecuteTransaction), varargs...)
}

// Read mocks base method
func (m *MockKeyValueServiceClient) Read(ctx context.Context, in *Ydb_KeyValue.ReadRequest, opts ...grpc.CallOption) (*Ydb_KeyValue.ReadResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Read", varargs...)
	ret0, _ := ret[0].(*Ydb_KeyValue.ReadResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read
func (mr *MockKeyValueServiceClientMockRecorder) Read(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockKeyValueServiceClient)(nil).Read), varargs...)
}

// ReadRange mocks base method
func (m *MockKeyValueServiceClient) ReadRange(ctx context.Context, in *Ydb_KeyValue.ReadRangeRequest, opts ...grpc.CallOption) (*Ydb_KeyValue.ReadRangeResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ReadRange", varargs...)
	ret0, _ := ret[0].(*Ydb_KeyValue.ReadRangeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadRange indicates an expected call of ReadRange
func (mr *MockKeyValueServiceClientMockRecorder) ReadRange(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadRange", reflect.TypeOf((*MockKeyValueServiceClient)(nil).ReadRange), varargs...)
}

// ListRange mocks base method
func (m *MockKeyValueServiceClient) ListRange(ctx context.Context, in *Ydb_KeyValue.ListRangeRequest, opts ...grpc.CallOption) (*Ydb_KeyValue.ListRangeResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListRange", varargs...)
	ret0, _ := ret[0].(*Ydb_KeyValue.ListRangeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListRange indicates an expected call of ListRange
func (mr *MockKeyValueServiceClientMockRecorder) ListRange(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListRange", reflect.TypeOf((*MockKeyValueServiceClient)(nil).ListRange), varargs...)
}

// GetStorageChannelStatus mocks base method
func (m *MockKeyValueServiceClient) GetStorageChannelStatus(ctx context.Context, in *Ydb_KeyValue.GetStorageChannelStatusRequest, opts ...grpc.CallOption) (*Ydb_KeyValue.GetStorageChannelStatusResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetStorageChannelStatus", varargs...)
	ret0, _ := ret[0].(*Ydb_KeyValue.GetStorageChannelStatusResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStorageChannelStatus indicates an expected call of GetStorageChannelStatus
func (mr *MockKeyValueServiceClientMockRecorder) GetStorageChannelStatus(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStorageChannelStatus", reflect.TypeOf((*MockKeyValueServiceClient)(nil).GetStorageChannelStatus), varargs...)
}

// MockKeyValueServiceServer is a mock of KeyValueServiceServer interface
type MockKeyValueServiceServer struct {
	ctrl     *gomock.Controller
	recorder *MockKeyValueServiceServerMockRecorder
}

// MockKeyValueServiceServerMockRecorder is the mock recorder for MockKeyValueServiceServer
type MockKeyValueServiceServerMockRecorder struct {
	mock *MockKeyValueServiceServer
}

// NewMockKeyValueServiceServer creates a new mock instance
func NewMockKeyValueServiceServer(ctrl *gomock.Controller) *MockKeyValueServiceServer {
	mock := &MockKeyValueServiceServer{ctrl: ctrl}
	mock.recorder = &MockKeyValueServiceServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockKeyValueServiceServer) EXPECT() *MockKeyValueServiceServerMockRecorder {
	return m.recorder
}

// CreateVolume mocks base method
func (m *MockKeyValueServiceServer) CreateVolume(arg0 context.Context, arg1 *Ydb_KeyValue.CreateVolumeRequest) (*Ydb_KeyValue.CreateVolumeResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateVolume", arg0, arg1)
	ret0, _ := ret[0].(*Ydb_KeyValue.CreateVolumeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateVolume indicates an expected call of CreateVolume
func (mr *MockKeyValueServiceServerMockRecorder) CreateVolume(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateVolume", reflect.TypeOf((*MockKeyValueServiceServer)(nil).CreateVolume), arg0, arg1)
}

// DropVolume mocks base method
func (m *MockKeyValueServiceServer) DropVolume(arg0 context.Context, arg1 *Ydb_KeyValue.DropVolumeRequest) (*Ydb_KeyValue.DropVolumeResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DropVolume", arg0, arg1)
	ret0, _ := ret[0].(*Ydb_KeyValue.DropVolumeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DropVolume indicates an expected call of DropVolume
func (mr *MockKeyValueServiceServerMockRecorder) DropVolume(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DropVolume", reflect.TypeOf((*MockKeyValueServiceServer)(nil).DropVolume), arg0, arg1)
}

// AlterVolume mocks base method
func (m *MockKeyValueServiceServer) AlterVolume(arg0 context.Context, arg1 *Ydb_KeyValue.AlterVolumeRequest) (*Ydb_KeyValue.AlterVolumeResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AlterVolume", arg0, arg1)
	ret0, _ := ret[0].(*Ydb_KeyValue.AlterVolumeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AlterVolume indicates an expected call of AlterVolume
func (mr *MockKeyValueServiceServerMockRecorder) AlterVolume(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AlterVolume", reflect.TypeOf((*MockKeyValueServiceServer)(nil).AlterVolume), arg0, arg1)
}

// DescribeVolume mocks base method
func (m *MockKeyValueServiceServer) DescribeVolume(arg0 context.Context, arg1 *Ydb_KeyValue.DescribeVolumeRequest) (*Ydb_KeyValue.DescribeVolumeResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeVolume", arg0, arg1)
	ret0, _ := ret[0].(*Ydb_KeyValue.DescribeVolumeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeVolume indicates an expected call of DescribeVolume
func (mr *MockKeyValueServiceServerMockRecorder) DescribeVolume(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeVolume", reflect.TypeOf((*MockKeyValueServiceServer)(nil).DescribeVolume), arg0, arg1)
}

// ListLocalPartitions mocks base method
func (m *MockKeyValueServiceServer) ListLocalPartitions(arg0 context.Context, arg1 *Ydb_KeyValue.ListLocalPartitionsRequest) (*Ydb_KeyValue.ListLocalPartitionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListLocalPartitions", arg0, arg1)
	ret0, _ := ret[0].(*Ydb_KeyValue.ListLocalPartitionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListLocalPartitions indicates an expected call of ListLocalPartitions
func (mr *MockKeyValueServiceServerMockRecorder) ListLocalPartitions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListLocalPartitions", reflect.TypeOf((*MockKeyValueServiceServer)(nil).ListLocalPartitions), arg0, arg1)
}

// AcquireLock mocks base method
func (m *MockKeyValueServiceServer) AcquireLock(arg0 context.Context, arg1 *Ydb_KeyValue.AcquireLockRequest) (*Ydb_KeyValue.AcquireLockResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AcquireLock", arg0, arg1)
	ret0, _ := ret[0].(*Ydb_KeyValue.AcquireLockResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AcquireLock indicates an expected call of AcquireLock
func (mr *MockKeyValueServiceServerMockRecorder) AcquireLock(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AcquireLock", reflect.TypeOf((*MockKeyValueServiceServer)(nil).AcquireLock), arg0, arg1)
}

// ExecuteTransaction mocks base method
func (m *MockKeyValueServiceServer) ExecuteTransaction(arg0 context.Context, arg1 *Ydb_KeyValue.ExecuteTransactionRequest) (*Ydb_KeyValue.ExecuteTransactionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecuteTransaction", arg0, arg1)
	ret0, _ := ret[0].(*Ydb_KeyValue.ExecuteTransactionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecuteTransaction indicates an expected call of ExecuteTransaction
func (mr *MockKeyValueServiceServerMockRecorder) ExecuteTransaction(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteTransaction", reflect.TypeOf((*MockKeyValueServiceServer)(nil).ExecuteTransaction), arg0, arg1)
}

// Read mocks base method
func (m *MockKeyValueServiceServer) Read(arg0 context.Context, arg1 *Ydb_KeyValue.ReadRequest) (*Ydb_KeyValue.ReadResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", arg0, arg1)
	ret0, _ := ret[0].(*Ydb_KeyValue.ReadResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read
func (mr *MockKeyValueServiceServerMockRecorder) Read(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockKeyValueServiceServer)(nil).Read), arg0, arg1)
}

// ReadRange mocks base method
func (m *MockKeyValueServiceServer) ReadRange(arg0 context.Context, arg1 *Ydb_KeyValue.ReadRangeRequest) (*Ydb_KeyValue.ReadRangeResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadRange", arg0, arg1)
	ret0, _ := ret[0].(*Ydb_KeyValue.ReadRangeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadRange indicates an expected call of ReadRange
func (mr *MockKeyValueServiceServerMockRecorder) ReadRange(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadRange", reflect.TypeOf((*MockKeyValueServiceServer)(nil).ReadRange), arg0, arg1)
}

// ListRange mocks base method
func (m *MockKeyValueServiceServer) ListRange(arg0 context.Context, arg1 *Ydb_KeyValue.ListRangeRequest) (*Ydb_KeyValue.ListRangeResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListRange", arg0, arg1)
	ret0, _ := ret[0].(*Ydb_KeyValue.ListRangeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListRange indicates an expected call of ListRange
func (mr *MockKeyValueServiceServerMockRecorder) ListRange(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListRange", reflect.TypeOf((*MockKeyValueServiceServer)(nil).ListRange), arg0, arg1)
}

// GetStorageChannelStatus mocks base method
func (m *MockKeyValueServiceServer) GetStorageChannelStatus(arg0 context.Context, arg1 *Ydb_KeyValue.GetStorageChannelStatusRequest) (*Ydb_KeyValue.GetStorageChannelStatusResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStorageChannelStatus", arg0, arg1)
	ret0, _ := ret[0].(*Ydb_KeyValue.GetStorageChannelStatusResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStorageChannelStatus indicates an expected call of GetStorageChannelStatus
func (mr *MockKeyValueServiceServerMockRecorder) GetStorageChannelStatus(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStorageChannelStatus", reflect.TypeOf((*MockKeyValueServiceServer)(nil).GetStorageChannelStatus), arg0, arg1)
}

// mustEmbedUnimplementedKeyValueServiceServer mocks base method
func (m *MockKeyValueServiceServer) mustEmbedUnimplementedKeyValueServiceServer() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "mustEmbedUnimplementedKeyValueServiceServer")
}

// mustEmbedUnimplementedKeyValueServiceServer indicates an expected call of mustEmbedUnimplementedKeyValueServiceServer
func (mr *MockKeyValueServiceServerMockRecorder) mustEmbedUnimplementedKeyValueServiceServer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "mustEmbedUnimplementedKeyValueServiceServer", reflect.TypeOf((*MockKeyValueServiceServer)(nil).mustEmbedUnimplementedKeyValueServiceServer))
}

// MockUnsafeKeyValueServiceServer is a mock of UnsafeKeyValueServiceServer interface
type MockUnsafeKeyValueServiceServer struct {
	ctrl     *gomock.Controller
	recorder *MockUnsafeKeyValueServiceServerMockRecorder
}

// MockUnsafeKeyValueServiceServerMockRecorder is the mock recorder for MockUnsafeKeyValueServiceServer
type MockUnsafeKeyValueServiceServerMockRecorder struct {
	mock *MockUnsafeKeyValueServiceServer
}

// NewMockUnsafeKeyValueServiceServer creates a new mock instance
func NewMockUnsafeKeyValueServiceServer(ctrl *gomock.Controller) *MockUnsafeKeyValueServiceServer {
	mock := &MockUnsafeKeyValueServiceServer{ctrl: ctrl}
	mock.recorder = &MockUnsafeKeyValueServiceServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockUnsafeKeyValueServiceServer) EXPECT() *MockUnsafeKeyValueServiceServerMockRecorder {
	return m.recorder
}

// mustEmbedUnimplementedKeyValueServiceServer mocks base method
func (m *MockUnsafeKeyValueServiceServer) mustEmbedUnimplementedKeyValueServiceServer() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "mustEmbedUnimplementedKeyValueServiceServer")
}

// mustEmbedUnimplementedKeyValueServiceServer indicates an expected call of mustEmbedUnimplementedKeyValueServiceServer
func (mr *MockUnsafeKeyValueServiceServerMockRecorder) mustEmbedUnimplementedKeyValueServiceServer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "mustEmbedUnimplementedKeyValueServiceServer", reflect.TypeOf((*MockUnsafeKeyValueServiceServer)(nil).mustEmbedUnimplementedKeyValueServiceServer))
}