package terraform_test

import (
	"fmt"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestMain(m *testing.M) {
	addr := os.Getenv("YDB_ACC_IAM_ACCESS_SERVICE_ADDR")
	if os.Getenv("TF_ACC") != "1" || addr == "" {
		os.Exit(m.Run())
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "IAM acceptance access service: failed to listen: %v\n", err)
		os.Exit(1)
	}
	server := grpc.NewServer(grpc.UnknownServiceHandler(func(_ interface{}, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		if method != "/yandex.cloud.priv.servicecontrol.v1.AccessService/Authorize" && method != "/yandex.cloud.priv.accessservice.v2.AccessService/Authorize" {
			return status.Error(codes.Unimplemented, "unexpected IAM RPC")
		}
		if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
			return err
		}
		return stream.SendMsg(&emptypb.Empty{})
	}))
	go func() { _ = server.Serve(listener) }()
	code := m.Run()
	server.Stop()
	os.Exit(code)
}
