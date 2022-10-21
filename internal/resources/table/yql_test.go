package table

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrepareCreateRequest(t *testing.T) {
	var testData = []struct {
		testName string
		resource *TableResource
		expected string
	}{
		{
			testName: "table with one column as PK",
			resource: &TableResource{
				Path: "privet",
				Columns: []*TableColumn{
					{
						Name: "mir",
						Type: "Utf8",
					},
				},
				PrimaryKey: &TablePrimaryKey{
					Columns: []string{
						"mir",
					},
				},
			},
			expected: "CREATE TABLE `privet`(" +
				"\n" +
				"\tmir Utf8," + "\n" +
				"\tPRIMARY KEY (`mir`)" + "\n" +
				")\n",
		},
		{
			testName: "table with two columns as PK and one as index",
			resource: &TableResource{
				Path: "privet",
				Columns: []*TableColumn{
					{
						Name: "mir",
						Type: "Utf8",
					},
					{
						Name: "vasya",
						Type: "Utf8",
					},
				},
				PrimaryKey: &TablePrimaryKey{
					Columns: []string{
						"mir", "vasya",
					},
				},
				Indexes: []*TableIndex{
					{
						Name: "indexname",
						Type: "global_async",
						Columns: []string{
							"vasya",
						},
					},
				},
			},
			expected: "CREATE TABLE `privet`(" + "\n" +
				"\tmir Utf8" + "," + "\n" +
				"\tvasya Utf8" + "," + "\n" +
				"\tINDEX `indexname` GLOBAL ASYNC ON (`vasya`)," + "\n" +
				"\tPRIMARY KEY (`mir`,`vasya`)" + "\n" +
				")\n",
		},
		{
			testName: "table with three columns and index with cover",
			resource: &TableResource{
				Path: "privet/hello",
				Columns: []*TableColumn{
					{
						Name: "mir",
						Type: "Utf8",
					},
					{
						Name: "vasya",
						Type: "Utf8",
					},
					{
						Name: "cover",
						Type: "Uint32",
					},
				},
				PrimaryKey: &TablePrimaryKey{
					Columns: []string{
						"mir", "vasya",
					},
				},
				Indexes: []*TableIndex{
					{
						Name: "indexname",
						Type: "global_async",
						Columns: []string{
							"vasya",
						},
						Cover: []string{
							"cover",
						},
					},
				},
			},
			expected: "CREATE TABLE `privet/hello`(" + "\n" +
				"\tmir Utf8" + "," + "\n" +
				"\tvasya Utf8" + "," + "\n" +
				"\tcover Uint32" + "," + "\n" +
				"\tINDEX `indexname` GLOBAL ASYNC ON (`vasya`) COVER (`cover`)," + "\n" +
				"\tPRIMARY KEY (`mir`,`vasya`)" + "\n" +
				")\n",
		},
		{
			testName: "table with two columns and two column-families",
			resource: &TableResource{
				Path: "hello/world",
				Columns: []*TableColumn{
					{
						Name:   "mir",
						Type:   "Utf8",
						Family: "some_family",
					},
					{
						Name: "vasya",
						Type: "Utf8",
					},
				},
				PrimaryKey: &TablePrimaryKey{
					Columns: []string{
						"mir",
					},
				},
				Family: []*TableFamily{
					{
						Name:        "some_family",
						Data:        "ssd",
						Compression: "lz4",
					},
					{
						Name:        "some_family_2",
						Data:        "ssd",
						Compression: "off",
					},
				},
			},
			expected: "CREATE TABLE `hello/world`(" + "\n" +
				"\tmir Utf8 FAMILY `some_family`," + "\n" +
				"\tvasya Utf8," + "\n" +
				"\tPRIMARY KEY (`mir`)," + "\n" +
				"\tFAMILY `some_family`(" + "\n" +
				"\t\tDATA = \"ssd\"," + "\n" +
				"\t\tCOMPRESSION = \"lz4\"" + "\n" +
				"\t)," + "\n" +
				"\tFAMILY `some_family_2`(" + "\n" +
				"\t\tDATA = \"ssd\"," + "\n" +
				"\t\tCOMPRESSION = \"off\"" + "\n" +
				"\t)" + "\n" +
				")\n",
		},
		{
			testName: "table with two columns with one as ttl",
			resource: &TableResource{
				Path: "hello/world",
				Columns: []*TableColumn{
					{
						Name: "mir",
						Type: "Utf8",
					},
					{
						Name: "ttl",
						Type: "Timestamp",
					},
				},
				PrimaryKey: &TablePrimaryKey{
					Columns: []string{
						"mir",
					},
				},
				TTL: &TableTTL{
					ColumnName: "ttl",
					Interval:   "PT0S",
				},
			},
			expected: "CREATE TABLE `hello/world`(" + "\n" +
				"\tmir Utf8," + "\n" +
				"\tttl Timestamp," + "\n" +
				"\tPRIMARY KEY (`mir`)" + "\n" +
				")" + "\n" +
				"WITH (" + "\n" +
				"\tTTL = Interval(\"PT0S\") ON `ttl`" + "\n" +
				")",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := PrepareCreateRequest(v.resource)
			assert.Equal(t, v.expected, got)
			fmt.Println(got)
		})
	}
}
