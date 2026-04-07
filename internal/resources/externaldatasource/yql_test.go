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
					"password_secret_name": "my_password_secret",
					"database_name":        "default",
					"protocol":             "NATIVE",
				},
				UseTLS: boolPtr(true),
			},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/ch_source`" +
				` WITH ( SOURCE_TYPE = "ClickHouse", LOCATION = "clickhouse-host:9000",` +
				` AUTH_METHOD = "BASIC", LOGIN = "user", PASSWORD_SECRET_NAME = "my_password_secret",` +
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
				"service_account_secret_name": "sa_secret",
			}},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/sa_source`" +
				` WITH ( SOURCE_TYPE = "ObjectStorage", LOCATION = "storage.yandexcloud.net",` +
				` AUTH_METHOD = "SERVICE_ACCOUNT", SERVICE_ACCOUNT_ID = "sa-id-123",` +
				` SERVICE_ACCOUNT_SECRET_NAME = "sa_secret" )`,
		},
		{
			testName: "with aws auth",
			fullPath: "/local/aws_source",
			resource: &Resource{Values: map[string]string{
				"source_type":                       "ObjectStorage",
				"location":                          "s3.us-east-1.amazonaws.com",
				"auth_method":                       "AWS",
				"aws_access_key_id_secret_name":     "aws_key_id",
				"aws_secret_access_key_secret_name": "aws_secret_key",
				"aws_region":                        "us-east-1",
			}},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/aws_source`" +
				` WITH ( SOURCE_TYPE = "ObjectStorage", LOCATION = "s3.us-east-1.amazonaws.com",` +
				` AUTH_METHOD = "AWS", AWS_ACCESS_KEY_ID_SECRET_NAME = "aws_key_id",` +
				` AWS_SECRET_ACCESS_KEY_SECRET_NAME = "aws_secret_key", AWS_REGION = "us-east-1" )`,
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
					"password_secret_name": "pg_pass",
					"database_name":        "mydb",
					"mdb_cluster_id":       "c9q1234567890",
				},
				UseTLS: boolPtr(true),
			},
			expected: `CREATE EXTERNAL DATA SOURCE ` + "`/local/mdb_source`" +
				` WITH ( SOURCE_TYPE = "PostgreSQL", LOCATION = "rc1a-xxx.mdb.yandexcloud.net:6432",` +
				` AUTH_METHOD = "MDB_BASIC", LOGIN = "pguser", PASSWORD_SECRET_NAME = "pg_pass",` +
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
