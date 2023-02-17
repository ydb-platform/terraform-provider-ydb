package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrepareDropIndexQuery(t *testing.T) {
	testData := []struct {
		testName    string
		tableName   string
		indexToDrop string
		expected    string
	}{
		{
			testName:    "index to drop without escape symbols",
			tableName:   "table",
			indexToDrop: "privet",
			expected:    "ALTER TABLE `table` DROP INDEX `privet`",
		},
		{
			testName:    "index to drop with escape symbols",
			tableName:   "table",
			indexToDrop: "\"privet",
			expected:    "ALTER TABLE `table` DROP INDEX `\\\"privet`",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := prepareDropRequest(v.tableName, v.indexToDrop)
			assert.Equal(t, v.expected, got)
		})
	}
}

func TestPrepareAddIndexQuery(t *testing.T) {
	testData := []struct {
		testName string
		index    *resource
		expected string
	}{
		{
			testName: "async index without covers",
			index: &resource{
				Name:      "index_name",
				TablePath: "table",
				Type:      "global_async",
				Columns: []string{
					"a", "b", "c",
				},
			},
			expected: "ALTER TABLE `table` ADD INDEX `index_name` GLOBAL ASYNC ON (`a`, `b`, `c`)",
		},
		{
			testName: "sync index without covers",
			index: &resource{
				Name:      "index_name",
				TablePath: "table",
				Type:      "global_sync",
				Columns: []string{
					"a", "b", "c",
				},
			},
			expected: "ALTER TABLE `table` ADD INDEX `index_name` GLOBAL SYNC ON (`a`, `b`, `c`)",
		},
		{
			testName: "async index with covers",
			index: &resource{
				Name:      "index_name",
				TablePath: "table",
				Type:      "global_async",
				Columns: []string{
					"a", "b", "c",
				},
				Cover: []string{
					"d", "e", "f",
				},
			},
			expected: "ALTER TABLE `table` ADD INDEX `index_name` GLOBAL ASYNC ON (`a`, `b`, `c`) COVER (`d`, `e`, `f`)",
		},
		{
			testName: "sync index with covers",
			index: &resource{
				TablePath: "table",
				Name:      "index_name",
				Type:      "global_sync",
				Columns: []string{
					"a", "b", "c",
				},
				Cover: []string{
					"d", "e", "f",
				},
			},
			expected: "ALTER TABLE `table` ADD INDEX `index_name` GLOBAL SYNC ON (`a`, `b`, `c`) COVER (`d`, `e`, `f`)",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := prepareCreateIndexRequest(v.index)
			assert.Equal(t, v.expected, got)
		})
	}
}
