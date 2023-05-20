package ratelimiter

import (
	"github.com/ydb-platform/terraform-provider-ydb/internal/resources"
)

type handlerRateLimiter struct {
	token string
}

func NewHandler(token string) resources.Handler {
	return &handlerRateLimiter{
		token: token,
	}
}
