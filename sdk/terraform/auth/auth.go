package auth

import "context"

type YdbCredentials struct {
	Token    string
	User     string
	Password string
}

type GetAuthCallback func(ctx context.Context) (YdbCredentials, error)
