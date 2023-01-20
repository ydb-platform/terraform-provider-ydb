package auth

import "context"

type GetTokenCallback func(ctx context.Context) (string, error)
