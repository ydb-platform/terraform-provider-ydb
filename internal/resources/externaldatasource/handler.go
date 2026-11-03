package externaldatasource

import (
	"context"
	"time"

	"github.com/ydb-platform/terraform-provider-ydb/internal/resources"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

// Handler implements resources.Handler and DataSourceRead for ydb_external_data_source.
type Handler struct {
	authCreds    auth.YdbCredentials
	openBackend  func(context.Context, string, auth.YdbCredentials) (backend, error)
	closeTimeout time.Duration
}

var _ resources.Handler = (*Handler)(nil)

func NewHandler(authCreds auth.YdbCredentials) *Handler {
	return &Handler{
		authCreds: authCreds,
	}
}
