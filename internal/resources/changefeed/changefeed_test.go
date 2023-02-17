package changefeed

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTablePathFromCDCEntity(t *testing.T) {
	testData := []struct {
		testName   string
		entityPath string
		expected   string
	}{
		{
			testName: "empty entity_path",
		},
		{
			testName:   "valid entity_path",
			entityPath: "ydb/table/changefeed",
			expected:   "ydb/table",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := parseTablePathFromCDCEntity(v.entityPath)
			assert.Equal(t, v.expected, got)
		})
	}
}
