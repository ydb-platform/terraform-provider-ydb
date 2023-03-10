package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseYDBEntityID(t *testing.T) {
	testData := []struct {
		testName    string
		id          string
		expected    *YDBEntity
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
			id:       "grpcs://lb.abacaba42.cloud.yandex.net:2135/?database=/pre-prod_ydb_public/abacaba/cabababa?path=topic/path",
			expected: &YDBEntity{
				databaseEndpoint: "lb.abacaba42.cloud.yandex.net:2135",
				database:         "/pre-prod_ydb_public/abacaba/cabababa",
				entityPath:       "topic/path",
				useTLS:           true,
			},
			expectedErr: false,
		},
		{
			testName: "valid localhost endpoint with topic path",
			id:       "grpc://localhost:2136/?database=/local?path=topic",
			expected: &YDBEntity{
				databaseEndpoint: "localhost:2136",
				database:         "/local",
				entityPath:       "topic",
				useTLS:           false,
			},
			expectedErr: false,
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got, err := ParseYDBEntityID(v.id)
			if !v.expectedErr {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			assert.Equal(t, got, v.expected)
		})
	}
}
