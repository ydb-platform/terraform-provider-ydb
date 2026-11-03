package externaldatasource

import (
	"context"
	"time"

	"github.com/ydb-platform/terraform-provider-ydb/internal/resources"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

// Handler implements resources.Handler and DataSourceRead for ydb_external_data_source.
type Handler struct {
	authCreds      auth.YdbCredentials
	callerAuth     auth.GetAuthCallback
	openBackend    func(context.Context, string, auth.YdbCredentials) (backend, error)
	uniqueName     func() string
	cleanupTimeout time.Duration
}

var _ resources.Handler = (*Handler)(nil)

func NewHandler(authCreds auth.YdbCredentials) *Handler {
	return &Handler{
		authCreds: authCreds,
	}
}

// NewHandlerWithCallerAuth retains the caller callback for fresh bootstrap and cleanup credentials.
func NewHandlerWithCallerAuth(creds auth.YdbCredentials, cb auth.GetAuthCallback) *Handler {
	h := NewHandler(creds)
	h.callerAuth = cb
	return h
}
