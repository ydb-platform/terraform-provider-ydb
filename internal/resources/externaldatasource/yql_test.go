package externaldatasource

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func boolPtr(b bool) *bool { return &b }

func TestPrepareCreateQuery(t *testing.T) {
	testData := []struct {
		testName string
		fullPath string
		resource *Resource
		expected string
	}{
		{
			testName: "minimal with source_type and location only",
			fullPath: "/local/my_source",
			resource: &Resource{Values: map[string]string{
				"source_type": "ObjectStorage",
				"location":    "localhost:12345",
			}},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/my_source`" +
				` WITH ( SOURCE_TYPE = "ObjectStorage", LOCATION = "localhost:12345" )`,
		},
		{
			testName: "with auth_method NONE",
			fullPath: "/local/s3_source",
			resource: &Resource{Values: map[string]string{
				"source_type": "ObjectStorage",
				"location":    "s3.amazonaws.com",
				"auth_method": "NONE",
			}},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/s3_source`" +
				` WITH ( SOURCE_TYPE = "ObjectStorage", LOCATION = "s3.amazonaws.com", AUTH_METHOD = "NONE" )`,
		},
		{
			testName: "clickhouse with basic auth",
			fullPath: "/local/ch_source",
			resource: &Resource{
				Values: map[string]string{
					"source_type":          "ClickHouse",
					"location":             "clickhouse-host:9000",
					"auth_method":          "BASIC",
					"login":                "user",
					"password_secret_path": "/local/my_password_secret",
					"database_name":        "default",
					"protocol":             "NATIVE",
				},
				UseTLS: boolPtr(true),
			},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/ch_source`" +
				` WITH ( SOURCE_TYPE = "ClickHouse", LOCATION = "clickhouse-host:9000",` +
				` AUTH_METHOD = "BASIC", LOGIN = "user", PASSWORD_SECRET_PATH = "/local/my_password_secret",` +
				` DATABASE_NAME = "default", PROTOCOL = "NATIVE", USE_TLS = "TRUE" )`,
		},
		{
			testName: "with service account auth",
			fullPath: "/local/sa_source",
			resource: &Resource{Values: map[string]string{
				"source_type":                 "ObjectStorage",
				"location":                    "storage.yandexcloud.net",
				"auth_method":                 "SERVICE_ACCOUNT",
				"service_account_id":          "sa-id-123",
				"service_account_secret_path": "/local/sa_secret",
			}},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/sa_source`" +
				` WITH ( SOURCE_TYPE = "ObjectStorage", LOCATION = "storage.yandexcloud.net",` +
				` AUTH_METHOD = "SERVICE_ACCOUNT", SERVICE_ACCOUNT_ID = "sa-id-123",` +
				` SERVICE_ACCOUNT_SECRET_PATH = "/local/sa_secret" )`,
		},
		{
			testName: "with aws auth",
			fullPath: "/local/aws_source",
			resource: &Resource{Values: map[string]string{
				"source_type":                       "ObjectStorage",
				"location":                          "s3.us-east-1.amazonaws.com",
				"auth_method":                       "AWS",
				"aws_access_key_id_secret_path":     "/local/aws_key_id",
				"aws_secret_access_key_secret_path": "/local/aws_secret_key",
				"aws_region":                        "us-east-1",
			}},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/aws_source`" +
				` WITH ( SOURCE_TYPE = "ObjectStorage", LOCATION = "s3.us-east-1.amazonaws.com",` +
				` AUTH_METHOD = "AWS", AWS_ACCESS_KEY_ID_SECRET_PATH = "/local/aws_key_id",` +
				` AWS_SECRET_ACCESS_KEY_SECRET_PATH = "/local/aws_secret_key", AWS_REGION = "us-east-1" )`,
		},
		{
			testName: "with mdb cluster",
			fullPath: "/local/mdb_source",
			resource: &Resource{
				Values: map[string]string{
					"source_type":          "PostgreSQL",
					"location":             "rc1a-xxx.mdb.yandexcloud.net:6432",
					"auth_method":          "MDB_BASIC",
					"login":                "pguser",
					"password_secret_path": "/local/pg_pass",
					"database_name":        "mydb",
					"mdb_cluster_id":       "c9q1234567890",
				},
				UseTLS: boolPtr(true),
			},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/mdb_source`" +
				` WITH ( SOURCE_TYPE = "PostgreSQL", LOCATION = "rc1a-xxx.mdb.yandexcloud.net:6432",` +
				` AUTH_METHOD = "MDB_BASIC", LOGIN = "pguser", PASSWORD_SECRET_PATH = "/local/pg_pass",` +
				` DATABASE_NAME = "mydb", MDB_CLUSTER_ID = "c9q1234567890", USE_TLS = "TRUE" )`,
		},
		{
			testName: "empty optional fields are omitted",
			fullPath: "/local/minimal",
			resource: &Resource{Values: map[string]string{
				"source_type": "ObjectStorage",
				"location":    "localhost:12345",
				"auth_method": "NONE",
			}},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/minimal`" +
				` WITH ( SOURCE_TYPE = "ObjectStorage", LOCATION = "localhost:12345", AUTH_METHOD = "NONE" )`,
		},
		{
			testName: "postgresql with schema",
			fullPath: "/local/pg_source",
			resource: &Resource{
				Values: map[string]string{
					"source_type":          "PostgreSQL",
					"location":             "localhost:5432",
					"auth_method":          "BASIC",
					"login":                "pguser",
					"password_secret_path": "/local/pg_pass",
					"database_name":        "mydb",
					"protocol":             "NATIVE",
					"schema":               "public",
				},
				UseTLS: boolPtr(true),
			},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/pg_source`" +
				` WITH ( SOURCE_TYPE = "PostgreSQL", LOCATION = "localhost:5432",` +
				` AUTH_METHOD = "BASIC", LOGIN = "pguser", PASSWORD_SECRET_PATH = "/local/pg_pass",` +
				` DATABASE_NAME = "mydb", PROTOCOL = "NATIVE", SCHEMA = "public", USE_TLS = "TRUE" )`,
		},
		{
			testName: "oracle with service_name",
			fullPath: "/local/oracle_source",
			resource: &Resource{Values: map[string]string{
				"source_type":          "Oracle",
				"location":             "localhost:1521",
				"auth_method":          "BASIC",
				"login":                "orauser",
				"password_secret_path": "/local/ora_pass",
				"database_name":        "ORCL",
				"service_name":         "my_service",
			}},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/oracle_source`" +
				` WITH ( SOURCE_TYPE = "Oracle", LOCATION = "localhost:1521",` +
				` AUTH_METHOD = "BASIC", LOGIN = "orauser", PASSWORD_SECRET_PATH = "/local/ora_pass",` +
				` DATABASE_NAME = "ORCL", SERVICE_NAME = "my_service" )`,
		},
		{
			testName: "solomon with properties",
			fullPath: "/local/solomon_source",
			resource: &Resource{
				Values: map[string]string{
					"source_type":       "Solomon",
					"location":          "localhost:9090",
					"auth_method":       "TOKEN",
					"token_secret_path": "/local/tok",
					"grpc_location":     "vla",
					"project":           "myproject",
					"cluster":           "production",
				},
				UseTLS: boolPtr(true),
			},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/solomon_source`" +
				` WITH ( SOURCE_TYPE = "Solomon", LOCATION = "localhost:9090",` +
				` AUTH_METHOD = "TOKEN", TOKEN_SECRET_PATH = "/local/tok",` +
				` GRPC_LOCATION = "vla", PROJECT = "myproject",` +
				` CLUSTER = "production", USE_TLS = "TRUE" )`,
		},
		{
			testName: "ydb with database_id",
			fullPath: "/local/ydb_source",
			resource: &Resource{Values: map[string]string{
				"source_type":   "Ydb",
				"location":      "localhost:2136",
				"auth_method":   "NONE",
				"database_name": "mydb",
				"database_id":   "etn123",
			}},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/ydb_source`" +
				` WITH ( SOURCE_TYPE = "Ydb", LOCATION = "localhost:2136",` +
				` AUTH_METHOD = "NONE", DATABASE_NAME = "mydb",` +
				` DATABASE_ID = "etn123" )`,
		},
		{
			testName: "logging with folder_id",
			fullPath: "/local/logging_source",
			resource: &Resource{Values: map[string]string{
				"source_type":                 "Logging",
				"location":                    "localhost:8080",
				"auth_method":                 "SERVICE_ACCOUNT",
				"service_account_id":          "sa123",
				"service_account_secret_path": "/local/sa_secret",
				"folder_id":                   "b1g456",
			}},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/logging_source`" +
				` WITH ( SOURCE_TYPE = "Logging", LOCATION = "localhost:8080",` +
				` AUTH_METHOD = "SERVICE_ACCOUNT", SERVICE_ACCOUNT_ID = "sa123",` +
				` SERVICE_ACCOUNT_SECRET_PATH = "/local/sa_secret", FOLDER_ID = "b1g456" )`,
		},
	}

	for _, v := range testData {
		t.Run(v.testName, func(t *testing.T) {
			got := PrepareCreateQuery(v.fullPath, v.resource)
			assert.Equal(t, v.expected, got)
		})
	}
}

func TestPrepareDropQuery(t *testing.T) {
	testData := []struct {
		testName string
		fullPath string
		expected string
	}{
		{
			testName: "simple path",
			fullPath: "/local/my_source",
			expected: "DROP EXTERNAL DATA SOURCE `/local/my_source`",
		},
		{
			testName: "nested path",
			fullPath: "/local/folder/nested_source",
			expected: "DROP EXTERNAL DATA SOURCE `/local/folder/nested_source`",
		},
	}

	for _, v := range testData {
		t.Run(v.testName, func(t *testing.T) {
			got := PrepareDropQuery(v.fullPath)
			assert.Equal(t, v.expected, got)
		})
	}
}
