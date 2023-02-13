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
			},
			expected: "CREATE TABLE `privet`(" + "\n" +
				"\t`mir` Utf8" + "," + "\n" +
				"\t`vasya` Utf8" + "," + "\n" +
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
			},
			expected: "CREATE TABLE `privet\\/hello`(" + "\n" +
				"\t`mir` Utf8" + "," + "\n" +
				"\t`vasya` Utf8" + "," + "\n" +
				"\t`cover` Uint32" + "," + "\n" +
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
	partitioningBySize := 42
	partitioningByLoadFalse := false
	partitioningByLoadTrue := true

	testData := []struct {
		testName            string
		tableName           string
		settings            *PartitioningSettings
		readReplicaSettings string
		expected            string
	}{
		{
			testName:            "only read_replica_settings are changed",
			tableName:           "abacaba",
			readReplicaSettings: "abacaba",
			expected: "ALTER TABLE `abacaba` SET (\n" +
				"READ_REPLICAS_SETTINGS = \"abacaba\"\n)",
		},
		{
			testName:  "enable only partitioning_by_size",
			tableName: "abacaba",
			settings: &PartitioningSettings{
				BySize: &partitioningBySize,
			},
			expected: "ALTER TABLE `abacaba` SET (\n" +
				"AUTO_PARTITIONING_BY_SIZE = ENABLED,\n" +
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 42\n)",
		},
		{
			testName:  "enable only partitioning_by_load",
			tableName: "abacaba",
			settings: &PartitioningSettings{
				ByLoad: &partitioningByLoadTrue,
			},
			expected: "ALTER TABLE `abacaba` SET (\n" +
				"AUTO_PARTITIONING_BY_LOAD = ENABLED\n)",
		},
		{
			testName:  "disable only partitioning_by_load",
			tableName: "abacaba",
			settings: &PartitioningSettings{
				ByLoad: &partitioningByLoadFalse,
			},
			expected: "ALTER TABLE `abacaba` SET (\n" +
				"AUTO_PARTITIONING_BY_LOAD = DISABLED\n)",
		},
		{
			testName:  "set only min_partitions_count",
			tableName: "abacaba",
			settings: &PartitioningSettings{
				MinPartitionsCount: 42,
			},
			expected: "ALTER TABLE `abacaba` SET (\n" +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 42\n)",
		},
		{
			testName:  "set min_partitions_count and max_partitions_count",
			tableName: "abacaba",
			settings: &PartitioningSettings{
				MinPartitionsCount: 5,
				MaxPartitionsCount: 42,
			},
			expected: "ALTER TABLE `abacaba` SET (\n" +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5,\n" +
				"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 42\n)",
		},
		{
			testName:  "all settings set",
			tableName: "abacaba",
			settings: &PartitioningSettings{
				BySize:             &partitioningBySize,
				ByLoad:             &partitioningByLoadTrue,
				MinPartitionsCount: 4,
				MaxPartitionsCount: 42,
			},
			readReplicaSettings: "abacaba",
			expected: "ALTER TABLE `abacaba` SET (\n" +
				"AUTO_PARTITIONING_BY_LOAD = ENABLED,\n" +
				"AUTO_PARTITIONING_BY_SIZE = ENABLED,\n" +
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 42,\n" +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4,\n" +
				"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 42,\n" +
				"READ_REPLICAS_SETTINGS = \"abacaba\"\n)",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := prepareNewPartitioningSettingsQuery(v.tableName, v.settings, v.readReplicaSettings)
			assert.Equal(t, v.expected, got)
		})
	}
}

//func TestPrepareCDCAlterQuery(t *testing.T) {
//	virtualTimestampsTrue := true
//	virtualTimestampsFalse := false
//	formatJSON := "JSON"
//	retentionPeriod := "PT12H"
//
//	testData := []struct {
//		testName  string
//		tableName string
//		cdc       []*ChangeDataCaptureSettings
//		expected  string
//	}{
//		{
//			testName:  "empty cdc",
//			tableName: "abacaba",
//			expected:  "",
//		},
//		{
//			testName:  "multiple cdcs",
//			tableName: "abacaba",
//			cdc: []*ChangeDataCaptureSettings{
//				{
//					Name:              "cdc",
//					Mode:              "UPDATES",
//					Format:            &formatJSON,
//					RetentionPeriod:   &retentionPeriod,
//					VirtualTimestamps: &virtualTimestampsTrue,
//				},
//				{
//					Name:              "cdc2",
//					Mode:              "UPDATES",
//					Format:            nil,
//					VirtualTimestamps: &virtualTimestampsFalse,
//				},
//			},
//			expected: "ALTER TABLE `abacaba` ADD CHANGEFEED `cdc` WITH (\n" +
//				"MODE = \"UPDATES\",\n" +
//				"FORMAT = \"JSON\",\n" +
//				"VIRTUAL_TIMESTAMPS = true,\n" +
//				"RETENTION_PERIOD = Interval(\"PT12H\")\n);\n" +
//				"ALTER TABLE `abacaba` ADD CHANGEFEED `cdc2` WITH (\n" +
//				"MODE = \"UPDATES\",\n" +
//				"VIRTUAL_TIMESTAMPS = false\n)",
//		},
//	}
//
//	for _, v := range testData {
//		v := v
//		t.Run(v.testName, func(t *testing.T) {
//			got := prepareCDCAlterQuery(v.tableName, v.cdc)
//			assert.Equal(t, v.expected, got)
//		})
//	}
//}

func TestPrepareAlterRequest(t *testing.T) {
	newPartitioningBySize := 42
	newPartitioningByLoad := false
	testData := []struct {
		testName string
		diff     *tableDiff
		expected string
	}{
		{
			testName: "no diff",
			diff:     nil,
			expected: "",
		},
		{
			testName: "only add columns",
			diff: &tableDiff{
				TableName: "abacaba",
				ColumnsToAdd: []*Column{
					{
						Name:    "a",
						Type:    "Bool",
						Family:  "my_very_own_family",
						NotNull: true,
					},
					{
						Name:    "b",
						Type:    "Utf8",
						Family:  "my_very_own_family",
						NotNull: true,
					},
				},
			},
			expected: "ALTER TABLE `abacaba` ADD COLUMN `a` Bool FAMILY `my_very_own_family` NOT NULL, ADD COLUMN `b` Utf8 FAMILY `my_very_own_family` NOT NULL",
		},
		{
			testName: "change only partitioning settings",
			diff: &tableDiff{
				TableName: "abacaba",
				NewPartitioningSettings: &PartitioningSettings{
					BySize:             &newPartitioningBySize,
					MinPartitionsCount: 1,
					MaxPartitionsCount: 5,
				},
			},
			expected: "ALTER TABLE `abacaba` SET (\n" +
				"AUTO_PARTITIONING_BY_SIZE = ENABLED,\n" +
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 42,\n" +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,\n" +
				"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 5\n)",
		},
		{
			testName: "change only read_replicas_settings",
			diff: &tableDiff{
				TableName:            "abacaba",
				ReadReplicasSettings: "abacaba",
			},
			expected: "ALTER TABLE `abacaba` SET (\n" +
				"READ_REPLICAS_SETTINGS = \"abacaba\"\n)",
		},
		{
			testName: "change only ttl settings",
			diff: &tableDiff{
				TableName: "abacaba",
				NewTTLSettings: &TTL{
					ColumnName:     "d",
					ExpireInterval: "PT0S",
				},
			},
			expected: "ALTER TABLE `abacaba` RESET (TTL);\n" +
				"ALTER TABLE `abacaba` SET (TTL = Interval(\"PT0S\") ON `d`)",
		},
		{
			testName: "change all settings",
			diff: &tableDiff{
				TableName: "abacaba",
				ColumnsToAdd: []*Column{
					{
						Name:    "a",
						Type:    "Bool",
						Family:  "my_family",
						NotNull: true,
					},
					{
						Name:    "b",
						Type:    "Utf8",
						Family:  "my_family",
						NotNull: true,
					},
				},
				NewTTLSettings: &TTL{
					ColumnName:     "d",
					ExpireInterval: "PT0S",
				},
				NewPartitioningSettings: &PartitioningSettings{
					BySize:             &newPartitioningBySize,
					ByLoad:             &newPartitioningByLoad,
					MinPartitionsCount: 4,
					MaxPartitionsCount: 42,
				},
				ReadReplicasSettings: "abacaba",
			},
			expected: "ALTER TABLE `abacaba` ADD COLUMN `a` Bool FAMILY `my_family` NOT NULL, ADD COLUMN `b` Utf8 FAMILY `my_family` NOT NULL;\n" +
				"ALTER TABLE `abacaba` RESET (TTL);\n" +
				"ALTER TABLE `abacaba` SET (TTL = Interval(\"PT0S\") ON `d`);\n" +
				"ALTER TABLE `abacaba` SET (\n" +
				"AUTO_PARTITIONING_BY_LOAD = DISABLED,\n" +
				"AUTO_PARTITIONING_BY_SIZE = ENABLED,\n" +
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 42,\n" +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4,\n" +
				"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 42,\n" +
				"READ_REPLICAS_SETTINGS = \"abacaba\"\n)",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := PrepareAlterRequest(v.diff)
			assert.Equal(t, v.expected, got)
		})
	}
}
