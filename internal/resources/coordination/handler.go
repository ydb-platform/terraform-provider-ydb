package coordination

import (
	"github.com/ydb-platform/terraform-provider-ydb/internal/resources"
)

type handlerCoordination struct {
	token string
}

func NewHandler(token string) resources.Handler {
	return &handlerCoordination{
		token: token,
	}
}
