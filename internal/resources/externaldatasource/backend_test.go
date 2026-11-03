package externaldatasource

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type fakeBackend struct {
	queries       []string
	exec          func(context.Context, string, []query.ExecuteOption) error
	desc          options.ExternalDataSourceDescription
	describeErr   error
	describeCalls int
	absent        bool
}

func (f *fakeBackend) Exec(ctx context.Context, q string, opts ...query.ExecuteOption) error {
	f.queries = append(f.queries, q)
	if f.exec != nil {
		return f.exec(ctx, q, opts)
	}
	return nil
}

func (f *fakeBackend) Describe(context.Context, string) (options.ExternalDataSourceDescription, error) {
	f.describeCalls++
	return f.desc, f.describeErr
}
func (f *fakeBackend) PathAbsent(context.Context, string) (bool, error) { return f.absent, nil }
func (f *fakeBackend) Close(context.Context) error                      { return nil }
