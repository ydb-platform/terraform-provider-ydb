package table

import (
	"context"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type TableClientParams struct {
	DatabaseEndpoint string
	Token            string
}

func CreateTableClient(ctx context.Context, params TableClientParams) (table.Client, error) {
	db, err := ydb.Open(ctx, params.DatabaseEndpoint, ydb.WithAccessTokenCredentials(params.Token))
	if err != nil {
		return nil, err
	}

	return db.Table(), nil
}
