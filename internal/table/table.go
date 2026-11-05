package table

import (
	"context"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

type ClientParams struct {
	DatabaseEndpoint  string
	AuthCreds         auth.YdbCredentials
	DisableSDKLogging bool
}

func CreateDBConnection(ctx context.Context, params ClientParams) (*ydb.Driver, error) {
	var opts []ydb.Option
	switch {
	case params.AuthCreds.Token != "":
		opts = append(opts, ydb.WithAccessTokenCredentials(params.AuthCreds.Token))
	case params.AuthCreds.User != "":
		opts = append(opts, ydb.WithStaticCredentials(params.AuthCreds.User, params.AuthCreds.Password))
	}
	if params.DisableSDKLogging {
		// SDK logs include raw server errors, which can echo secret-bearing SQL.
		// Override its environment logger before any request is sent.
		opts = append(opts, ydb.WithLogger(nil, trace.Details(0)))
	}

	db, err := ydb.Open(ctx, params.DatabaseEndpoint, opts...)
	if err != nil {
		return nil, err
	}
	return db, nil
}
