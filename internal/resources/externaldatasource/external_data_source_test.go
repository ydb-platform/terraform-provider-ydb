package externaldatasource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func res(m map[string]string) *Resource {
	if m == nil {
		return &Resource{}
	}
	return &Resource{Values: m}
}

func TestValidateResourceAuth(t *testing.T) {
	tests := []struct {
		name    string
		r       *Resource
		wantErr string
	}{
		// NONE
		{
			name: "NONE valid",
			r:    res(map[string]string{"auth_method": "NONE"}),
		},
		{
			name:    "NONE with login",
			r:       res(map[string]string{"auth_method": "NONE", "login": "user"}),
			wantErr: `LOGIN is not supported for AUTH_METHOD = "NONE"`,
		},
		{
			name:    "NONE with aws param",
			r:       res(map[string]string{"auth_method": "NONE", "aws_access_key_id_secret_name": "key"}),
			wantErr: `AWS_ACCESS_KEY_ID_SECRET_NAME is not supported for AUTH_METHOD = "NONE"`,
		},

		// BASIC
		{
			name: "BASIC valid with secret name",
			r: res(map[string]string{
				"auth_method": "BASIC", "login": "user", "password_secret_name": "pass",
			}),
		},
		{
			name: "BASIC valid with secret path",
			r: res(map[string]string{
				"auth_method": "BASIC", "login": "user", "password_secret_path": "/path",
			}),
		},
		{
			name:    "BASIC missing login",
			r:       res(map[string]string{"auth_method": "BASIC", "password_secret_name": "pass"}),
			wantErr: `LOGIN is required for AUTH_METHOD = "BASIC"`,
		},
		{
			name:    "BASIC missing password secret",
			r:       res(map[string]string{"auth_method": "BASIC", "login": "user"}),
			wantErr: `either PASSWORD_SECRET_NAME or PASSWORD_SECRET_PATH is required for AUTH_METHOD = "BASIC"`,
		},
		{
			name: "BASIC both secret name and path",
			r: res(map[string]string{
				"auth_method": "BASIC", "login": "user",
				"password_secret_name": "n", "password_secret_path": "p",
			}),
			wantErr: "cannot specify both PASSWORD_SECRET_NAME and PASSWORD_SECRET_PATH",
		},
		{
			name: "BASIC with aws param",
			r: res(map[string]string{
				"auth_method": "BASIC", "login": "user", "password_secret_name": "p", "aws_region": "us-east-1",
			}),
			wantErr: `AWS_REGION is not supported for AUTH_METHOD = "BASIC"`,
		},
		{
			name: "BASIC with mdb_cluster_id",
			r: res(map[string]string{
				"auth_method": "BASIC", "login": "user", "password_secret_name": "p", "mdb_cluster_id": "c9q",
			}),
			wantErr: `MDB_CLUSTER_ID is not supported for AUTH_METHOD = "BASIC"`,
		},

		// MDB_BASIC
		{
			name: "MDB_BASIC valid",
			r: res(map[string]string{
				"auth_method": "MDB_BASIC", "service_account_id": "sa", "login": "user",
				"service_account_secret_name": "sa_s", "password_secret_name": "pass",
			}),
		},
		{
			name: "MDB_BASIC valid with mdb_cluster_id",
			r: res(map[string]string{
				"auth_method": "MDB_BASIC", "service_account_id": "sa", "login": "user",
				"service_account_secret_name": "sa_s", "password_secret_name": "pass",
				"mdb_cluster_id": "c9q123",
			}),
		},
		{
			name: "MDB_BASIC missing service_account_id",
			r: res(map[string]string{
				"auth_method": "MDB_BASIC", "login": "user",
				"service_account_secret_name": "sa_s", "password_secret_name": "pass",
			}),
			wantErr: `SERVICE_ACCOUNT_ID is required for AUTH_METHOD = "MDB_BASIC"`,
		},
		{
			name: "MDB_BASIC mixed secret types",
			r: res(map[string]string{
				"auth_method": "MDB_BASIC", "service_account_id": "sa", "login": "user",
				"service_account_secret_name": "n", "password_secret_path": "/p",
			}),
			wantErr: "cannot mix secret name and secret path references",
		},

		// AWS
		{
			name: "AWS valid with secret names",
			r: res(map[string]string{
				"auth_method": "AWS", "aws_region": "us-east-1",
				"aws_access_key_id_secret_name": "key", "aws_secret_access_key_secret_name": "secret",
			}),
		},
		{
			name: "AWS valid with secret paths",
			r: res(map[string]string{
				"auth_method": "AWS", "aws_region": "us-east-1",
				"aws_access_key_id_secret_path": "/key", "aws_secret_access_key_secret_path": "/secret",
			}),
		},
		{
			name: "AWS missing region",
			r: res(map[string]string{
				"auth_method":                   "AWS",
				"aws_access_key_id_secret_name": "key", "aws_secret_access_key_secret_name": "secret",
			}),
			wantErr: `AWS_REGION is required for AUTH_METHOD = "AWS"`,
		},
		{
			name: "AWS missing access key",
			r: res(map[string]string{
				"auth_method": "AWS", "aws_region": "us-east-1", "aws_secret_access_key_secret_name": "secret",
			}),
			wantErr: `either AWS_ACCESS_KEY_ID_SECRET_NAME or AWS_ACCESS_KEY_ID_SECRET_PATH is required`,
		},
		{
			name: "AWS mixed secret types",
			r: res(map[string]string{
				"auth_method": "AWS", "aws_region": "us-east-1",
				"aws_access_key_id_secret_name": "key", "aws_secret_access_key_secret_path": "/secret",
			}),
			wantErr: "cannot mix secret name and secret path references",
		},

		// TOKEN
		{
			name: "TOKEN valid with name",
			r:    res(map[string]string{"auth_method": "TOKEN", "token_secret_name": "tok"}),
		},
		{
			name: "TOKEN valid with path",
			r:    res(map[string]string{"auth_method": "TOKEN", "token_secret_path": "/tok"}),
		},
		{
			name:    "TOKEN missing secret",
			r:       res(map[string]string{"auth_method": "TOKEN"}),
			wantErr: `either TOKEN_SECRET_NAME or TOKEN_SECRET_PATH is required`,
		},
		{
			name:    "TOKEN with login",
			r:       res(map[string]string{"auth_method": "TOKEN", "token_secret_name": "tok", "login": "user"}),
			wantErr: `LOGIN is not supported for AUTH_METHOD = "TOKEN"`,
		},

		// SERVICE_ACCOUNT
		{
			name: "SERVICE_ACCOUNT valid",
			r: res(map[string]string{
				"auth_method": "SERVICE_ACCOUNT", "service_account_id": "sa", "service_account_secret_name": "sec",
			}),
		},
		{
			name:    "SERVICE_ACCOUNT missing id",
			r:       res(map[string]string{"auth_method": "SERVICE_ACCOUNT", "service_account_secret_name": "sec"}),
			wantErr: `SERVICE_ACCOUNT_ID is required for AUTH_METHOD = "SERVICE_ACCOUNT"`,
		},
		{
			name:    "SERVICE_ACCOUNT missing secret",
			r:       res(map[string]string{"auth_method": "SERVICE_ACCOUNT", "service_account_id": "sa"}),
			wantErr: `either SERVICE_ACCOUNT_SECRET_NAME or SERVICE_ACCOUNT_SECRET_PATH is required`,
		},

		// Edge cases
		{
			name: "empty auth_method and no auth fields",
			r:    res(nil),
		},
		{
			name:    "empty auth_method with login",
			r:       res(map[string]string{"login": "user"}),
			wantErr: "auth_method is required when login, secrets, or other auth-related attributes are set",
		},
		{
			name:    "empty auth_method with aws secret",
			r:       res(map[string]string{"aws_access_key_id_secret_name": "key"}),
			wantErr: "auth_method is required when login, secrets, or other auth-related attributes are set",
		},
		{
			name:    "unknown auth_method",
			r:       res(map[string]string{"auth_method": "UNKNOWN"}),
			wantErr: `unknown AUTH_METHOD "UNKNOWN"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateResourceAuth(tt.r)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
