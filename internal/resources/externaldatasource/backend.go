package externaldatasource

import (
	"context"
	"path"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

// backend isolates database access from resource lifecycle handling.
type backend interface {
	Exec(context.Context, string, ...query.ExecuteOption) error
	Describe(context.Context, string) (options.ExternalDataSourceDescription, error)
	PathAbsent(context.Context, string) (bool, error)
	Close(context.Context) error
}
type driverBackend struct{ *ydb.Driver }

func (b driverBackend) Exec(ctx context.Context, sql string, opts ...query.ExecuteOption) error {
	return b.Query().Exec(ctx, sql, opts...)
}

func (b driverBackend) Describe(ctx context.Context, p string) (options.ExternalDataSourceDescription, error) {
	d, err := b.Table().DescribeExternalDataSource(ctx, p)
	if err != nil {
		return options.ExternalDataSourceDescription{}, err
	}
	return *d, nil
}

func (b driverBackend) PathAbsent(ctx context.Context, p string) (bool, error) {
	// A successful listing distinguishes exact absence from permission/transport errors
	// and from generic SCHEME_ERROR, which does not necessarily mean NotFound.
	dir, err := b.Scheme().ListDirectory(ctx, path.Dir(p))
	if err != nil {
		return false, err
	}
	for _, child := range dir.Children {
		if child.Name == path.Base(p) {
			return false, nil
		}
	}
	return true, nil
}

func openDriverBackend(ctx context.Context, endpoint string, creds auth.YdbCredentials) (backend, error) {
	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{DatabaseEndpoint: endpoint, AuthCreds: creds})
	if err != nil {
		return nil, err
	}
	return driverBackend{db}, nil
}

func (h *Handler) open(ctx context.Context, endpoint string) (backend, error) {
	if h.openBackend != nil {
		return h.openBackend(ctx, endpoint, h.authCreds)
	}
	return openDriverBackend(ctx, endpoint, h.authCreds)
}
