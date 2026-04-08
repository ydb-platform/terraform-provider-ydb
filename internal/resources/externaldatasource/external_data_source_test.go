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
			r:       res(map[string]string{"auth_method": "NONE", "aws_access_key_id_secret_path": "/key"}),
			wantErr: `AWS_ACCESS_KEY_ID_SECRET_PATH is not supported for AUTH_METHOD = "NONE"`,
		},

		// BASIC
		{
			name: "BASIC valid",
			r: res(map[string]string{
				"auth_method": "BASIC", "login": "user", "password_secret_path": "/pass",
			}),
		},
		{
			name:    "BASIC missing login",
			r:       res(map[string]string{"auth_method": "BASIC", "password_secret_path": "/pass"}),
			wantErr: `LOGIN is required for AUTH_METHOD = "BASIC"`,
		},
		{
			name:    "BASIC missing password secret",
			r:       res(map[string]string{"auth_method": "BASIC", "login": "user"}),
			wantErr: `PASSWORD_SECRET_PATH is required for AUTH_METHOD = "BASIC"`,
		},
		{
			name: "BASIC with aws param",
			r: res(map[string]string{
				"auth_method": "BASIC", "login": "user", "password_secret_path": "/p", "aws_region": "us-east-1",
			}),
			wantErr: `AWS_REGION is not supported for AUTH_METHOD = "BASIC"`,
		},
		{
			name: "BASIC with mdb_cluster_id",
			r: res(map[string]string{
				"auth_method": "BASIC", "login": "user", "password_secret_path": "/p", "mdb_cluster_id": "c9q",
			}),
			wantErr: `MDB_CLUSTER_ID is not supported for AUTH_METHOD = "BASIC"`,
		},

		// MDB_BASIC
		{
			name: "MDB_BASIC valid",
			r: res(map[string]string{
				"auth_method": "MDB_BASIC", "service_account_id": "sa", "login": "user",
				"service_account_secret_path": "/sa_s", "password_secret_path": "/pass",
			}),
		},
		{
			name: "MDB_BASIC valid with mdb_cluster_id",
			r: res(map[string]string{
				"auth_method": "MDB_BASIC", "service_account_id": "sa", "login": "user",
				"service_account_secret_path": "/sa_s", "password_secret_path": "/pass",
				"mdb_cluster_id": "c9q123",
			}),
		},
		{
			name: "MDB_BASIC missing service_account_id",
			r: res(map[string]string{
				"auth_method": "MDB_BASIC", "login": "user",
				"service_account_secret_path": "/sa_s", "password_secret_path": "/pass",
			}),
			wantErr: `SERVICE_ACCOUNT_ID is required for AUTH_METHOD = "MDB_BASIC"`,
		},

		// AWS
		{
			name: "AWS valid",
			r: res(map[string]string{
				"auth_method": "AWS", "aws_region": "us-east-1",
				"aws_access_key_id_secret_path": "/key", "aws_secret_access_key_secret_path": "/secret",
			}),
		},
		{
			name: "AWS missing region",
			r: res(map[string]string{
				"auth_method":                       "AWS",
				"aws_access_key_id_secret_path": "/key", "aws_secret_access_key_secret_path": "/secret",
			}),
			wantErr: `AWS_REGION is required for AUTH_METHOD = "AWS"`,
		},
		{
			name: "AWS missing access key",
			r: res(map[string]string{
				"auth_method": "AWS", "aws_region": "us-east-1", "aws_secret_access_key_secret_path": "/secret",
			}),
			wantErr: `AWS_ACCESS_KEY_ID_SECRET_PATH is required`,
		},

		// TOKEN
		{
			name: "TOKEN valid",
			r:    res(map[string]string{"auth_method": "TOKEN", "token_secret_path": "/tok"}),
		},
		{
			name:    "TOKEN missing secret",
			r:       res(map[string]string{"auth_method": "TOKEN"}),
			wantErr: `TOKEN_SECRET_PATH is required`,
		},
		{
			name:    "TOKEN with login",
			r:       res(map[string]string{"auth_method": "TOKEN", "token_secret_path": "/tok", "login": "user"}),
			wantErr: `LOGIN is not supported for AUTH_METHOD = "TOKEN"`,
		},

		// SERVICE_ACCOUNT
		{
			name: "SERVICE_ACCOUNT valid",
			r: res(map[string]string{
				"auth_method": "SERVICE_ACCOUNT", "service_account_id": "sa", "service_account_secret_path": "/sec",
			}),
		},
		{
			name:    "SERVICE_ACCOUNT missing id",
			r:       res(map[string]string{"auth_method": "SERVICE_ACCOUNT", "service_account_secret_path": "/sec"}),
			wantErr: `SERVICE_ACCOUNT_ID is required for AUTH_METHOD = "SERVICE_ACCOUNT"`,
		},
		{
			name:    "SERVICE_ACCOUNT missing secret",
			r:       res(map[string]string{"auth_method": "SERVICE_ACCOUNT", "service_account_id": "sa"}),
			wantErr: `SERVICE_ACCOUNT_SECRET_PATH is required`,
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
			r:       res(map[string]string{"aws_access_key_id_secret_path": "/key"}),
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

func TestValidateSourceType(t *testing.T) {
	tests := []struct {
		name    string
		r       *Resource
		wantErr string
	}{
		// Valid auth methods per source type.
		{
			name: "ClickHouse with BASIC",
			r:    res(map[string]string{"source_type": "ClickHouse", "auth_method": "BASIC"}),
		},
		{
			name: "ClickHouse with MDB_BASIC",
			r:    res(map[string]string{"source_type": "ClickHouse", "auth_method": "MDB_BASIC"}),
		},
		{
			name: "Ydb with NONE",
			r:    res(map[string]string{"source_type": "Ydb", "auth_method": "NONE"}),
		},
		{
			name: "Ydb with TOKEN",
			r:    res(map[string]string{"source_type": "Ydb", "auth_method": "TOKEN"}),
		},
		{
			name: "Ydb with SERVICE_ACCOUNT",
			r:    res(map[string]string{"source_type": "Ydb", "auth_method": "SERVICE_ACCOUNT"}),
		},
		{
			name: "YT with NONE",
			r:    res(map[string]string{"source_type": "YT", "auth_method": "NONE"}),
		},
		{
			name: "YT with TOKEN",
			r:    res(map[string]string{"source_type": "YT", "auth_method": "TOKEN"}),
		},
		{
			name: "Logging with SERVICE_ACCOUNT",
			r:    res(map[string]string{"source_type": "Logging", "auth_method": "SERVICE_ACCOUNT"}),
		},
		{
			name: "Solomon with NONE",
			r:    res(map[string]string{"source_type": "Solomon", "auth_method": "NONE"}),
		},
		{
			name: "MsSQLServer with BASIC",
			r:    res(map[string]string{"source_type": "MsSQLServer", "auth_method": "BASIC"}),
		},
		{
			name: "ObjectStorage with AWS",
			r:    res(map[string]string{"source_type": "ObjectStorage", "auth_method": "AWS"}),
		},
		{
			name: "YdbTopics with TOKEN",
			r:    res(map[string]string{"source_type": "YdbTopics", "auth_method": "TOKEN"}),
		},

		// Invalid auth methods per source type.
		{
			name:    "ClickHouse with AWS",
			r:       res(map[string]string{"source_type": "ClickHouse", "auth_method": "AWS"}),
			wantErr: `AUTH_METHOD "AWS" is not supported for SOURCE_TYPE "ClickHouse"`,
		},
		{
			name:    "ClickHouse with TOKEN",
			r:       res(map[string]string{"source_type": "ClickHouse", "auth_method": "TOKEN"}),
			wantErr: `AUTH_METHOD "TOKEN" is not supported for SOURCE_TYPE "ClickHouse"`,
		},
		{
			name:    "PostgreSQL with SERVICE_ACCOUNT",
			r:       res(map[string]string{"source_type": "PostgreSQL", "auth_method": "SERVICE_ACCOUNT"}),
			wantErr: `AUTH_METHOD "SERVICE_ACCOUNT" is not supported for SOURCE_TYPE "PostgreSQL"`,
		},
		{
			name:    "YT with BASIC",
			r:       res(map[string]string{"source_type": "YT", "auth_method": "BASIC"}),
			wantErr: `AUTH_METHOD "BASIC" is not supported for SOURCE_TYPE "YT"`,
		},
		{
			name:    "Logging with BASIC",
			r:       res(map[string]string{"source_type": "Logging", "auth_method": "BASIC"}),
			wantErr: `AUTH_METHOD "BASIC" is not supported for SOURCE_TYPE "Logging"`,
		},
		{
			name:    "MsSQLServer with MDB_BASIC",
			r:       res(map[string]string{"source_type": "MsSQLServer", "auth_method": "MDB_BASIC"}),
			wantErr: `AUTH_METHOD "MDB_BASIC" is not supported for SOURCE_TYPE "MsSQLServer"`,
		},
		{
			name:    "Redis with TOKEN",
			r:       res(map[string]string{"source_type": "Redis", "auth_method": "TOKEN"}),
			wantErr: `AUTH_METHOD "TOKEN" is not supported for SOURCE_TYPE "Redis"`,
		},

		// Valid properties per source type.
		{
			name: "PostgreSQL with schema",
			r:    res(map[string]string{"source_type": "PostgreSQL", "schema": "public"}),
		},
		{
			name: "PostgreSQL with database_name and protocol",
			r:    res(map[string]string{"source_type": "PostgreSQL", "database_name": "mydb", "protocol": "NATIVE"}),
		},
		{
			name: "Greenplum with schema",
			r:    res(map[string]string{"source_type": "Greenplum", "schema": "public"}),
		},
		{
			name: "Oracle with service_name",
			r:    res(map[string]string{"source_type": "Oracle", "service_name": "ORCL"}),
		},
		{
			name: "Logging with folder_id",
			r:    res(map[string]string{"source_type": "Logging", "folder_id": "b1g123"}),
		},
		{
			name: "Solomon with grpc_location, project, cluster",
			r:    res(map[string]string{"source_type": "Solomon", "grpc_location": "loc", "project": "proj", "cluster": "cls"}),
		},
		{
			name: "Ydb with database_id",
			r:    res(map[string]string{"source_type": "Ydb", "database_id": "db1"}),
		},
		{
			name: "MongoDB with reading_mode",
			r:    res(map[string]string{"source_type": "MongoDB", "reading_mode": "primary"}),
		},
		{
			name: "ClickHouse with use_tls",
			r:    &Resource{Values: map[string]string{"source_type": "ClickHouse"}, UseTLS: boolPtr(true)},
		},

		// Invalid properties per source type.
		{
			name:    "ObjectStorage with database_name",
			r:       res(map[string]string{"source_type": "ObjectStorage", "database_name": "db"}),
			wantErr: `DATABASE_NAME is not supported for SOURCE_TYPE "ObjectStorage"`,
		},
		{
			name:    "ObjectStorage with protocol",
			r:       res(map[string]string{"source_type": "ObjectStorage", "protocol": "NATIVE"}),
			wantErr: `PROTOCOL is not supported for SOURCE_TYPE "ObjectStorage"`,
		},
		{
			name:    "ClickHouse with schema",
			r:       res(map[string]string{"source_type": "ClickHouse", "schema": "public"}),
			wantErr: `SCHEMA is not supported for SOURCE_TYPE "ClickHouse"`,
		},
		{
			name:    "MySQL with protocol",
			r:       res(map[string]string{"source_type": "MySQL", "protocol": "NATIVE"}),
			wantErr: `PROTOCOL is not supported for SOURCE_TYPE "MySQL"`,
		},
		{
			name:    "YT with database_name",
			r:       res(map[string]string{"source_type": "YT", "database_name": "db"}),
			wantErr: `DATABASE_NAME is not supported for SOURCE_TYPE "YT"`,
		},
		{
			name:    "Logging with database_name",
			r:       res(map[string]string{"source_type": "Logging", "database_name": "db"}),
			wantErr: `DATABASE_NAME is not supported for SOURCE_TYPE "Logging"`,
		},
		{
			name:    "ObjectStorage with use_tls",
			r:       &Resource{Values: map[string]string{"source_type": "ObjectStorage"}, UseTLS: boolPtr(true)},
			wantErr: `USE_TLS is not supported for SOURCE_TYPE "ObjectStorage"`,
		},
		{
			name:    "Solomon with database_name",
			r:       res(map[string]string{"source_type": "Solomon", "database_name": "db"}),
			wantErr: `DATABASE_NAME is not supported for SOURCE_TYPE "Solomon"`,
		},

		// No source_type — skip validation.
		{
			name: "empty source_type skips validation",
			r:    res(map[string]string{"database_name": "db"}),
		},
		// No auth_method — skip auth×source_type validation.
		{
			name: "source_type without auth_method",
			r:    res(map[string]string{"source_type": "ClickHouse"}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSourceType(tt.r)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
