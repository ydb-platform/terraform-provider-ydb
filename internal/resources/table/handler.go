package table

import (
	"github.com/ydb-platform/terraform-provider-ydb/internal/resources"
)

type handler struct {
	token string
}

func NewHandler(token string) resources.Handler {
	return &handler{
		token: token,
	}
}
