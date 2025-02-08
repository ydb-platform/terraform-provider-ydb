package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestParseYDBDatabaseEndpoint(t *testing.T) {
	testData := []struct {
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
		{
			testName:             "valid localhost endpoint",
			endpoint:             "grpc://localhost:2136/?database=/local",
			expectedErr:          false,
			expectedBaseEndpoint: "localhost:2136",
			expectedUseTLS:       false,
			expectedDatabasePath: "/local",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			gotEp, gotDatabasePath, gotUseTLS, gotErr := ParseYDBDatabaseEndpoint(v.endpoint)
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

func TestConsumerSort(t *testing.T) {
	sch := []interface{}{
		map[string]interface{}{
			"name": "cons1",
			"supported_codecs": []interface{}{
				"gzip", // 2
				"raw",  // 1
				// "zstd", // 4
			},
		},
		map[string]interface{}{
			"name": "cons2",
			"supported_codecs": []interface{}{
				"gzip",
				"raw",
			},
		},
	}
	consDesc := []topictypes.Consumer{
		{
			Name: "cons2",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				4,
			},
		},
		{
			Name: "cons1",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				4,
			},
		},
		{
			Name: "cons3",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				4,
			},
		},
		{
			Name: "cons4",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				4,
			},
		},
	}

	res := ConsumerSort(sch, consDesc)
	assert.Equal(t, []topictypes.Consumer{
		{
			Name: "cons1",
			SupportedCodecs: []topictypes.Codec{
				2,
				1,
				4,
			},
		},
		{
			Name: "cons2",
			SupportedCodecs: []topictypes.Codec{
				2,
				1,
				4,
			},
		},
		{
			Name: "cons3",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				4,
			},
		},
		{
			Name: "cons4",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				4,
			},
		},
	}, res)
}

func TestAreAllElementsUnique(t *testing.T) {
	consDesc := []topictypes.Consumer{
		{
			Name: "cons2",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				4,
			},
		},
		{
			Name: "cons1",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				4,
			},
		},
	}
	consDescDup := []topictypes.Consumer{
		{
			Name: "cons2",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				4,
			},
		},
		{
			Name: "cons2",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				4,
			},
		},
	}
	consDescCodecDup := []topictypes.Consumer{
		{
			Name: "cons2",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				4,
			},
		},
		{
			Name: "cons1",
			SupportedCodecs: []topictypes.Codec{
				1,
				2,
				2,
			},
		},
	}
	assert.NoError(t, AreAllElementsUnique(consDesc))
	assert.Error(t, AreAllElementsUnique(consDescDup))
	assert.Error(t, AreAllElementsUnique(consDescCodecDup))
}
