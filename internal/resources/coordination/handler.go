package coordination

import (
	"github.com/ydb-platform/terraform-provider-ydb/internal/resources"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

type handlerCoordination struct {
	authCreds auth.YdbCredentials
}

func NewHandler(authCreds auth.YdbCredentials) resources.Handler {
	return &handlerCoordination{
		authCreds: authCreds,
	}
}
