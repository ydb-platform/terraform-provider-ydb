package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrepareCreateRequest(t *testing.T) {
	testData := []struct {
		testName string
		resource *Resource
		expected string
	}{
		{
			testName: "table with one column as PK",
			resource: &Resource{
				FullPath: "privet",
				Columns: []*Column{
					{
						Name: "mir",
						Type: "Utf8",
					},
				},
				PrimaryKey: &PrimaryKey{
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
			resource: &Resource{
				FullPath: "privet",
				Columns: []*Column{
					{
						Name: "mir",
						Type: "Utf8",
					},
					{
						Name: "vasya",
						Type: "Utf8",
					},
				},
				PrimaryKey: &PrimaryKey{
					Columns: []string{
						"mir", "vasya",
					},
				},
				Indexes: []*Index{
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
			resource: &Resource{
				FullPath: "privet/hello",
				Columns: []*Column{
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
				PrimaryKey: &PrimaryKey{
					Columns: []string{
						"mir", "vasya",
					},
				},
				Indexes: []*Index{
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
			expected: "CREATE TABLE `privet\\/hello`(" + "\n" +
				"\tmir Utf8" + "," + "\n" +
				"\tvasya Utf8" + "," + "\n" +
				"\tcover Uint32" + "," + "\n" +
				"\tINDEX `indexname` GLOBAL ASYNC ON (`vasya`) COVER (`cover`)," + "\n" +
				"\tPRIMARY KEY (`mir`,`vasya`)" + "\n" +
				")\n",
		},
		{
			testName: "table with two columns and two column-families",
			resource: &Resource{
				FullPath: "hello/world",
				Columns: []*Column{
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
				PrimaryKey: &PrimaryKey{
					Columns: []string{
						"mir",
					},
				},
				Family: []*Family{
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
			expected: "CREATE TABLE `hello\\/world`(" + "\n" +
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
			resource: &Resource{
				FullPath: "hello/world",
				Columns: []*Column{
					{
						Name: "mir",
						Type: "Utf8",
					},
					{
						Name: "ttl",
						Type: "Timestamp",
					},
				},
				PrimaryKey: &PrimaryKey{
					Columns: []string{
						"mir",
					},
				},
				TTL: &TTL{
					ColumnName:     "ttl",
					ExpireInterval: "PT0S",
				},
			},
			expected: "CREATE TABLE `hello\\/world`(" + "\n" +
				"\tmir Utf8," + "\n" +
				"\tttl Timestamp," + "\n" +
				"\tPRIMARY KEY (`mir`)" + "\n" +
				")" + "\n" +
				"WITH (" + "\n" +
				"\tTTL = Interval(\"PT0S\") ON `ttl`" + "\n" +
				")",
		},
		{
			testName: "table with two columns and partitioning settings",
			resource: &Resource{
				FullPath: "hello/world",
				Columns: []*Column{
					{
						Name: "mir",
						Type: "Utf8",
					},
					{
						Name:    "ttl",
						Type:    "Timestamp",
						NotNull: true,
					},
				},
				PrimaryKey: &PrimaryKey{
					Columns: []string{
						"mir",
					},
				},
				PartitioningSettings: &PartitioningSettings{
					PartitionsCount:    5,
					MaxPartitionsCount: 42,
					MinPartitionsCount: 10,
				},
			},
			expected: "CREATE TABLE `hello\\/world`(" + "\n" +
				"\tmir Utf8," + "\n" +
				"\tttl Timestamp NOT NULL," + "\n" +
				"\tPRIMARY KEY (`mir`)" + "\n" +
				")" + "\n" +
				"WITH (" + "\n" +
				"\tUNIFORM_PARTITIONS = 5," + "\n" +
				"\tAUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10," + "\n" +
				"\tAUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 42" + "\n" +
				")",
		},
		{
			testName: "table with replica settings",
			resource: &Resource{
				FullPath: "hello/world",
				Columns: []*Column{
					{
						Name: "mir",
						Type: "Utf8",
					},
					{
						Name:    "ttl",
						Type:    "Timestamp",
						NotNull: true,
					},
				},
				PrimaryKey: &PrimaryKey{
					Columns: []string{
						"mir",
					},
				},
				ReplicationSettings: &ReplicationSettings{
					ReadReplicasSettings: "PER_AZ",
				},
			},
			expected: "CREATE TABLE `hello\\/world`(" + "\n" +
				"\tmir Utf8," + "\n" +
				"\tttl Timestamp NOT NULL," + "\n" +
				"\tPRIMARY KEY (`mir`)" + "\n" +
				")" + "\n" +
				"WITH (" + "\n" +
				"\tREAD_REPLICAS_SETTINGS = \"PER_AZ\"" + "\n" +
				")",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := PrepareCreateRequest(v.resource)
			assert.Equal(t, v.expected, got)
		})
	}
}
