package externaltable

import (
	"github.com/ydb-platform/terraform-provider-ydb/internal/resources"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

// Handler implements resources.Handler for ydb_external_table.
type Handler struct {
	authCreds auth.YdbCredentials
}

var _ resources.Handler = (*Handler)(nil)

func NewHandler(authCreds auth.YdbCredentials) *Handler {
	return &Handler{
		authCreds: authCreds,
	}
}
