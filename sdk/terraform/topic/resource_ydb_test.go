package topic

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseYDBDatabaseEndpoint(t *testing.T) {
	var testData = []struct {
		testName             string
		endpoint             string
		expectedBaseEndpoint string
		expectedDatabasePath string
		expectedUseTLS       bool
		expectedErr          bool
	}{
		{
			testName:    "empty endpoint",
			endpoint:    "",
			expectedErr: true,
		},
		{
			testName:    "endpoint without grpc(s) prefix",
			endpoint:    "ydb.yandex-team.ru/?database=/some_database/path",
			expectedErr: true,
		},
		{
			testName:    "only hostname endpoint",
			endpoint:    "ydb.yandex-team.ru",
			expectedErr: true,
		},
		{
			testName:    "endpoint without database",
			endpoint:    "grpcs://ydb.yandex-team.ru",
			expectedErr: true,
		},
		{
			testName:             "valid grpcs endpoint",
			endpoint:             "grpcs://ydb.yandex-team.ru/?database=/some_database_path",
			expectedDatabasePath: "/some_database_path",
			expectedBaseEndpoint: "ydb.yandex-team.ru",
			expectedUseTLS:       true,
			expectedErr:          false,
		},
		{
			testName:             "valid grpc endpoint",
			endpoint:             "grpc://ydb.yandex-team.ru/?database=/some/path",
			expectedDatabasePath: "/some/path",
			expectedBaseEndpoint: "ydb.yandex-team.ru",
			expectedUseTLS:       false,
			expectedErr:          false,
		},
		{
			testName:    "valid endpoint with invalid protocol",
			endpoint:    "grp://ydb.yandex-team.ru/?database=/some/path",
			expectedErr: true,
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			gotEp, gotDatabasePath, gotUseTLS, gotErr := parseYDBDatabaseEndpoint(v.endpoint)
			if gotErr != nil && !v.expectedErr {
				t.Errorf("got err %q, but expected <nil>", gotErr)
			}
			if gotErr == nil && v.expectedErr {
				t.Error("got <nil> err, but expected")
			}
			if gotEp != v.expectedBaseEndpoint {
				t.Errorf("got base_endpoint %q, but expected %q", gotEp, v.expectedBaseEndpoint)
			}
			if gotDatabasePath != v.expectedDatabasePath {
				t.Errorf("got database_path %q, but expected %q", gotDatabasePath, v.expectedDatabasePath)
			}
			if gotUseTLS != v.expectedUseTLS {
				t.Errorf("got use_tls %v, but expected %v", gotUseTLS, v.expectedUseTLS)
			}
		})
	}
}

func TestParseYcpYDBEntityID(t *testing.T) {
	var testData = []struct {
		testName    string
		id          string
		expected    *ydbEntity
		expectedErr bool
	}{
		{
			testName:    "empty id",
			id:          "",
			expected:    nil,
			expectedErr: true,
		},
		{
			testName:    "valid endpoint without topic path",
			id:          "grpcs://lb.abacaba42.cloud.yandex.net:2135/?database=/pre-prod_ydb_public/abacaba/cabababa",
			expected:    nil,
			expectedErr: true,
		},
		{
			testName:    "valid endpoint with trailing slash",
			id:          "grpcs://lb.abacaba42.cloud.yandex.net:2135/?database=/pre-prod_ydb_public/abacaba/cabababa/",
			expected:    nil,
			expectedErr: true,
		},
		{
			testName: "valid endpoint with topic path",
			id:       "grpcs://lb.abacaba42.cloud.yandex.net:2135/?database=/pre-prod_ydb_public/abacaba/cabababa/topic/path",
			expected: &ydbEntity{
				databaseEndpoint: "lb.abacaba42.cloud.yandex.net:2135",
				database:         "/pre-prod_ydb_public/abacaba/cabababa",
				entityPath:       "topic/path",
				useTLS:           true,
			},
			expectedErr: false,
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got, err := parseYDBEntityID(v.id)
			if !v.expectedErr {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			assert.Equal(t, got, v.expected)
		})
	}
}
