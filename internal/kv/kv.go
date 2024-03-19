package kv

import (
	"context"
	"crypto/x509"

	"github.com/ydb-platform/ydb-go-genproto/draft/Ydb_KeyValue_V1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

type ClientParams struct {
	DatabaseEndpoint string
	Database         string
	UseTLS           bool
	AuthCreds        auth.YdbCredentials
}

func CreateDBConnection(ctx context.Context, params ClientParams) (*grpc.ClientConn, error) {
	var opts grpc.DialOption

	if params.UseTLS {
		pool, _ := x509.SystemCertPool()
		creds := credentials.NewClientTLSFromCert(pool, "")
		opts = grpc.WithTransportCredentials(creds)
	} else {
		opts = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	return grpc.Dial(params.DatabaseEndpoint, opts)
}

func AddMetaDataKvStub(ctx context.Context, metaParams ClientParams, conn *grpc.ClientConn) (context.Context, Ydb_KeyValue_V1.KeyValueServiceClient) {
	m := metadata.New(map[string]string{
		"x-ydb-database":    metaParams.Database,
		"x-ydb-auth-ticket": metaParams.AuthCreds.Token,
	})
	ctx = metadata.NewOutgoingContext(ctx, m)

	return ctx, Ydb_KeyValue_V1.NewKeyValueServiceClient(conn)
}
