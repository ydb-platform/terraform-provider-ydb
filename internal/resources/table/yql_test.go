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
				"\t`mir` Utf8," + "\n" +
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
				"\t`mir` Utf8" + "," + "\n" +
				"\t`vasya` Utf8" + "," + "\n" +
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
				"\t`mir` Utf8" + "," + "\n" +
				"\t`vasya` Utf8" + "," + "\n" +
				"\t`cover` Uint32" + "," + "\n" +
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
				"\t`mir` Utf8 FAMILY `some_family`," + "\n" +
				"\t`vasya` Utf8," + "\n" +
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
				"\t`mir` Utf8," + "\n" +
				"\t`ttl` Timestamp," + "\n" +
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
				"\t`mir` Utf8," + "\n" +
				"\t`ttl` Timestamp NOT NULL," + "\n" +
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
				"\t`mir` Utf8," + "\n" +
				"\t`ttl` Timestamp NOT NULL," + "\n" +
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
			got := prepareDropIndexQuery(v.tableName, v.indexToDrop)
			assert.Equal(t, v.expected, got)
		})
	}
}

func TestPrepareAddIndexQuery(t *testing.T) {
	testData := []struct {
		testName  string
		tableName string
		index     Index
		expected  string
	}{
		{
			testName:  "async index without covers",
			tableName: "table",
			index: Index{
				Name: "index_name",
				Type: "global_async",
				Columns: []string{
					"a", "b", "c",
				},
			},
			expected: "ALTER TABLE `table` ADD INDEX `index_name` GLOBAL ASYNC ON (`a`, `b`, `c`)",
		},
		{
			testName:  "sync index without covers",
			tableName: "table",
			index: Index{
				Name: "index_name",
				Type: "global_sync",
				Columns: []string{
					"a", "b", "c",
				},
			},
			expected: "ALTER TABLE `table` ADD INDEX `index_name` GLOBAL SYNC ON (`a`, `b`, `c`)",
		},
		{
			testName:  "async index with covers",
			tableName: "table",
			index: Index{
				Name: "index_name",
				Type: "global_async",
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
			testName:  "sync index with covers",
			tableName: "table",
			index: Index{
				Name: "index_name",
				Type: "global_sync",
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
			got := prepareAddIndexQuery(v.tableName, &v.index)
			assert.Equal(t, v.expected, got)
		})
	}
}

func TestPrepareAddColumnsQuery(t *testing.T) {
	testData := []struct {
		testName  string
		tableName string
		columns   []*Column
		expected  string
	}{
		{
			testName:  "single column without family",
			tableName: "abacaba",
			columns: []*Column{
				{
					Name:    "a",
					Type:    "Bool",
					NotNull: true,
				},
			},
			expected: "ALTER TABLE `abacaba` ADD COLUMN `a` Bool NOT NULL",
		},
		{
			testName:  "single column with family",
			tableName: "abacaba",
			columns: []*Column{
				{
					Name:    "a",
					Type:    "Bool",
					Family:  "family",
					NotNull: true,
				},
			},
			expected: "ALTER TABLE `abacaba` ADD COLUMN `a` Bool FAMILY `family` NOT NULL",
		},
		{
			testName:  "multiple columns with family",
			tableName: "abacaba",
			columns: []*Column{
				{
					Name:    "a",
					Type:    "Bool",
					Family:  "some_family",
					NotNull: true,
				},
				{
					Name:    "b",
					Type:    "Uint8",
					Family:  "some_family_2",
					NotNull: true,
				},
				{
					Name:    "c",
					Type:    "Uint16",
					Family:  "some_family_3",
					NotNull: false,
				},
			},
			expected: "ALTER TABLE `abacaba` ADD COLUMN" +
				" `a` Bool FAMILY `some_family` NOT NULL," +
				" ADD COLUMN `b` Uint8 FAMILY `some_family_2` NOT NULL," +
				" ADD COLUMN `c` Uint16 FAMILY `some_family_3`",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := prepareAddColumnsQuery(v.tableName, v.columns)
			assert.Equal(t, v.expected, got)
		})
	}
}

func TestPrepareDropColumnsQuery(t *testing.T) {
	testData := []struct {
		testName  string
		tableName string
		columns   []string
		expected  string
	}{
		{
			testName:  "single column",
			tableName: "abacaba",
			columns: []string{
				"a",
			},
			expected: "ALTER TABLE `abacaba` DROP COLUMN `a`",
		},
		{
			testName:  "multiple columns",
			tableName: "abacaba",
			columns: []string{
				"ab", "ba",
			},
			expected: "ALTER TABLE `abacaba` DROP COLUMN `ab`, DROP COLUMN `ba`",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := prepareDropColumnsQuery(v.tableName, v.columns)
			assert.Equal(t, v.expected, got)
		})
	}
}

func TestPrepareResetTTLQuery(t *testing.T) {
	testData := []struct {
		testName  string
		tableName string
		expected  string
	}{
		{
			testName:  "simple test",
			tableName: "table",
			expected:  "ALTER TABLE `table` RESET (TTL)",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := prepareResetTTLQuery(v.tableName)
			assert.Equal(t, v.expected, got)
		})
	}
}

func TestPrepareSetNewTTLSettingsQuery(t *testing.T) {
	testData := []struct {
		testName    string
		tableName   string
		ttlSettings *TTL
		expected    string
	}{
		{
			testName:  "simple test",
			tableName: "table",
			ttlSettings: &TTL{
				ColumnName:     "abacaba",
				ExpireInterval: "Never",
			},
			expected: "ALTER TABLE `table` SET (TTL = Interval(\"Never\") ON `abacaba`)",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := prepareSetNewTTLSettingsQuery(v.tableName, v.ttlSettings)
			assert.Equal(t, v.expected, got)
		})
	}
}

func TestPrepareNewPartitioningSettingsQuery(t *testing.T) {
	testData := []struct {
		testName string
	}{}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
		})
	}
}

func TestPrepareAlterRequest(t *testing.T) {
	testData := []struct {
		testName string
	}{}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {

		})
	}
}
