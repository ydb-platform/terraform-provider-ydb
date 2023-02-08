package table

import (
	"bytes"
	"strconv"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

const (
	defaultRequestCapacity = 1024 // 1 KiB
)

func appendIndent(req []byte, indent int) []byte {
	req = append(req, bytes.Repeat([]byte{'\t'}, indent)...)
	return req
}

func PrepareCreateRequest(r *Resource) string { //nolint:gocyclo
	req := make([]byte, 0, defaultRequestCapacity)

	req = append(req, "CREATE TABLE `"...)
	req = helpers.AppendWithEscape(req, r.FullPath)
	req = append(req, "`("...)
	req = append(req, '\n')

	indent := 1
	for _, v := range r.Columns {
		req = appendIndent(req, indent)
		req = append(req, v.ToYQL()...)
		req = append(req, ',')
		req = append(req, '\n')
	}

	haveIndex := false
	for _, v := range r.Indexes {
		if haveIndex {
			req = append(req, ',')
			req = append(req, '\n')
		}
		haveIndex = true
		req = appendIndent(req, indent)
		req = append(req, "INDEX"...)
		req = append(req, ' ')
		req = append(req, '`')
		req = helpers.AppendWithEscape(req, v.Name)
		req = append(req, '`')
		req = append(req, ' ')
		req = append(req, "GLOBAL"...)
		if v.Type == "global_async" { // TODO(shmel1k@): to consts
			req = append(req, " ASYNC"...)
		} else {
			req = append(req, " SYNC"...)
		}
		req = append(req, " ON "...)
		req = append(req, '(')
		for _, c := range v.Columns {
			req = append(req, '`')
			req = helpers.AppendWithEscape(req, c)
			req = append(req, '`', ',')
		}
		req[len(req)-1] = ')' // NOTE(shmel1k@): remove last column
		if len(v.Cover) > 0 {
			req = append(req, ' ')
			req = append(req, "COVER"...)
			req = append(req, ' ')
			req = append(req, '(')
			for _, c := range v.Cover {
				req = append(req, '`')
				req = helpers.AppendWithEscape(req, c)
				req = append(req, '`')
				req = append(req, ',')
			}
			req[len(req)-1] = ')'
		}
	}
	if len(r.Indexes) > 0 {
		req = append(req, ',')
		req = append(req, '\n')
	}

	req = appendIndent(req, indent)
	req = append(req, "PRIMARY KEY"...)
	req = append(req, ' ')
	req = append(req, '(')
	for _, v := range r.PrimaryKey.Columns {
		req = append(req, '`')
		req = helpers.AppendWithEscape(req, v)
		req = append(req, '`')
		req = append(req, ',')
	}
	req[len(req)-1] = ')'
	req = append(req, '\n')
	if len(r.Family) > 0 {
		req[len(req)-1] = ','
		for _, v := range r.Family {
			req = append(req, '\n')
			req = appendIndent(req, indent)
			req = append(req, "FAMILY"...)
			req = append(req, ' ')
			req = append(req, '`')
			req = helpers.AppendWithEscape(req, v.Name)
			req = append(req, '`')
			req = append(req, '(')
			req = append(req, '\n')
			indent++
			req = appendIndent(req, indent)
			req = append(req, "DATA = "...)
			req = append(req, '"')
			req = helpers.AppendWithEscape(req, v.Data)
			req = append(req, '"')
			req = append(req, ',')
			req = append(req, '\n')
			req = appendIndent(req, indent)
			req = append(req, "COMPRESSION = "...)
			req = append(req, '"')
			req = helpers.AppendWithEscape(req, v.Compression)
			req = append(req, '"')
			req = append(req, '\n')
			indent--
			req = appendIndent(req, indent)
			req = append(req, ')')
			req = append(req, ',')
		}
		req[len(req)-1] = '\n'
	}
	req = append(req, ')')
	req = append(req, '\n')
	indent--

	needWith := false
	if r.TTL != nil {
		needWith = true
	}
	if len(r.Attributes) != 0 {
		needWith = true
	}
	if r.PartitioningSettings != nil {
		needWith = true
	}
	if r.ReplicationSettings != nil {
		needWith = true
	}

	if !needWith {
		return string(req)
	}

	req = append(req, "WITH"...)
	req = append(req, ' ', '(', '\n')
	indent++
	needComma := false
	if r.TTL != nil {
		req = appendIndent(req, indent)
		req = append(req, "TTL = Interval(\""...)
		req = helpers.AppendWithEscape(req, r.TTL.ExpireInterval)
		req = append(req, '"')
		req = append(req, ')')
		req = append(req, " ON "...)
		req = append(req, '`')
		req = helpers.AppendWithEscape(req, r.TTL.ColumnName)
		req = append(req, '`')
		needComma = true
	}
	if r.PartitioningSettings != nil { //nolint:nestif
		if r.PartitioningSettings.ByLoad != nil {
			if needComma {
				req = append(req, ',', '\n')
			}
			req = appendIndent(req, indent)
			if *r.PartitioningSettings.ByLoad {
				req = append(req, "AUTO_PARTITIONING_BY_LOAD = ENABLED"...)
			} else {
				req = append(req, "AUTO_PARTITIONING_BY_LOAD = DISABLED"...)
			}
			needComma = true
		}
		if r.PartitioningSettings.BySize != nil {
			if needComma {
				req = append(req, ',', '\n')
			}
			req = appendIndent(req, indent)
			req = append(req, "AUTO_PARTITIONING_BY_SIZE = ENABLED"...)
			req = append(req, ',')
			req = append(req, '\n')
			req = appendIndent(req, indent)
			req = append(req, "AUTO_PARTITIONING_PARTITION_SIZE_MB = "...)
			req = strconv.AppendInt(req, int64(*r.PartitioningSettings.BySize), 10)
			needComma = true
		}
		if r.PartitioningSettings.PartitionsCount != 0 {
			if needComma {
				req = append(req, ',', '\n')
			}
			req = appendIndent(req, indent)
			req = append(req, "UNIFORM_PARTITIONS = "...)
			req = strconv.AppendInt(req, int64(r.PartitioningSettings.PartitionsCount), 10)
			needComma = true
		}
		if len(r.PartitioningSettings.PartitionAtKeys) != 0 {
			if needComma {
				req = append(req, ',', '\n')
			}
			req = appendIndent(req, indent)
			req = append(req, "PARTITION_AT_KEYS = "...)
			if len(r.PartitioningSettings.PartitionAtKeys) > 1 {
				req = append(req, '(')
			}
			for i, v := range r.PartitioningSettings.PartitionAtKeys {
				req = append(req, '(')
				for ii, vv := range v.Keys {
					switch t := vv.(type) {
					case uint64:
						req = strconv.AppendUint(req, t, 10)
					case int64:
						req = strconv.AppendInt(req, t, 10)
					case bool:
						req = strconv.AppendBool(req, t)
					case string:
						req = append(req, '"')
						req = helpers.AppendWithEscape(req, t)
						req = append(req, '"')
					}
					if ii < len(v.Keys)-1 {
						req = append(req, ',')
					}
				}
				req = append(req, ')')
				if i < len(r.PartitioningSettings.PartitionAtKeys)-1 {
					req = append(req, ',')
				}
			}
			if len(r.PartitioningSettings.PartitionAtKeys) > 1 {
				req = append(req, ')')
			}
			needComma = true
		}
		if r.PartitioningSettings.MinPartitionsCount != 0 {
			if needComma {
				req = append(req, ',', '\n')
			}
			req = appendIndent(req, indent)
			req = append(req, "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = "...)
			req = strconv.AppendInt(req, int64(r.PartitioningSettings.MinPartitionsCount), 10)
			needComma = true
		}
		if r.PartitioningSettings.MaxPartitionsCount != 0 {
			if needComma {
				req = append(req, ',', '\n')
			}
			req = appendIndent(req, indent)
			req = append(req, "AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = "...)
			req = strconv.AppendInt(req, int64(r.PartitioningSettings.MaxPartitionsCount), 10)
			needComma = true
		}
	}
	if r.ReplicationSettings != nil && r.ReplicationSettings.ReadReplicasSettings != "" {
		if needComma {
			req = append(req, ',', '\n')
		}
		req = appendIndent(req, indent)
		req = append(req, "READ_REPLICAS_SETTINGS = \""...)
		req = helpers.AppendWithEscape(req, r.ReplicationSettings.ReadReplicasSettings)
		req = append(req, '"')
		needComma = true
	}
	if r.EnableBloomFilter != nil {
		if needComma {
			req = append(req, ',', '\n')
		}
		needComma = true
		req = appendIndent(req, indent)
		req = append(req, "KEY_BLOOM_FILTER = "...)
		if *r.EnableBloomFilter {
			req = append(req, "ENABLED"...)
		} else {
			req = append(req, "DISABLED"...)
		}
	}

	// indent--
	_ = needComma

	req = append(req, '\n', ')')

	//	if len(r.ChangeFeeds) > 0 {
	//		req = append(req, ';', '\n')
	//		req = append(req, prepareCDCAlterQuery(r.Path, r.ChangeFeeds)...)
	//	}

	return string(req)
}

func prepareDropIndexQuery(tableName string, indexToDrop string) string {
	req := []byte("ALTER TABLE `")
	req = helpers.AppendWithEscape(req, tableName)
	req = append(req, '`', ' ')
	req = append(req, "DROP INDEX `"...)
	req = helpers.AppendWithEscape(req, indexToDrop)
	req = append(req, '`')
	return string(req)
}

func prepareAddIndexQuery(tableName string, indexToAdd *Index) string {
	req := []byte("ALTER TABLE `")
	req = helpers.AppendWithEscape(req, tableName)
	req = append(req, '`', ' ')
	req = append(req, "ADD INDEX `"...)
	req = helpers.AppendWithEscape(req, indexToAdd.Name)
	req = append(req, '`', ' ')
	// TODO(shmel1k@): add ToYQL for index
	if indexToAdd.Type == "global_async" { // TODO(shmel1k@): move to consts
		req = append(req, "GLOBAL ASYNC ON ("...)
	} else {
		req = append(req, "GLOBAL SYNC ON ("...)
	}
	for i := 0; i < len(indexToAdd.Columns); i++ {
		req = append(req, '`')
		req = helpers.AppendWithEscape(req, indexToAdd.Columns[i])
		req = append(req, '`')
		if i != len(indexToAdd.Columns)-1 {
			req = append(req, ',', ' ')
		}
	}
	req = append(req, ')')
	if len(indexToAdd.Cover) > 0 {
		req = append(req, " COVER ("...)
		for i := 0; i < len(indexToAdd.Cover); i++ {
			req = append(req, '`')
			req = helpers.AppendWithEscape(req, indexToAdd.Cover[i])
			req = append(req, '`')
			if i != len(indexToAdd.Cover)-1 {
				req = append(req, ',', ' ')
			}
		}
		req = append(req, ')')
	}

	return string(req)
}

func prepareAddColumnsQuery(tableName string, columnsToAdd []*Column) string {
	req := []byte("ALTER TABLE `")
	req = helpers.AppendWithEscape(req, tableName)
	req = append(req, '`', ' ')
	for i := 0; i < len(columnsToAdd); i++ {
		req = append(req, "ADD COLUMN "...)
		req = append(req, columnsToAdd[i].ToYQL()...)
		if i != len(columnsToAdd)-1 {
			req = append(req, ',', ' ')
		}
	}

	return string(req)
}

func prepareDropColumnsQuery(tableName string, columnsToDrop []string) string {
	req := make([]byte, 0, defaultRequestCapacity)
	req = append(req, "ALTER TABLE `"...)
	req = helpers.AppendWithEscape(req, tableName)
	req = append(req, '`', ' ')
	for i := 0; i < len(columnsToDrop); i++ {
		req = append(req, "DROP COLUMN `"...)
		req = helpers.AppendWithEscape(req, columnsToDrop[i])
		req = append(req, '`')
		if i != len(columnsToDrop)-1 {
			req = append(req, ',', ' ')
		}
	}

	return string(req)
}

func prepareResetTTLQuery(tableName string) string {
	buf := make([]byte, 0, 64)
	buf = append(buf, "ALTER TABLE `"...)
	buf = helpers.AppendWithEscape(buf, tableName)
	buf = append(buf, '`', ' ')
	buf = append(buf, "RESET (TTL)"...)
	return string(buf)
}

func prepareSetNewTTLSettingsQuery(tableName string, settings *TTL) string {
	buf := make([]byte, 0, 64)
	buf = append(buf, "ALTER TABLE `"...)
	buf = helpers.AppendWithEscape(buf, tableName)
	buf = append(buf, '`', ' ')
	buf = append(buf, "SET ("...)
	buf = append(buf, settings.ToYQL()...)
	buf = append(buf, ')')
	return string(buf)
}

func prepareKeyBloomFilterQuery(tableName string, enabled bool) string {
	buf := make([]byte, 0, 64)
	buf = append(buf, "ALTER TABLE `"...)
	buf = helpers.AppendWithEscape(buf, tableName)
	buf = append(buf, '`', ' ')
	buf = append(buf, "SET (\n"...)
	buf = append(buf, "KEY_BLOOM_FILTER = "...)
	if enabled {
		buf = append(buf, "ENABLED"...)
	} else {
		buf = append(buf, "DISABLED"...)
	}
	buf = append(buf, ')')
	return string(buf)
}

func prepareNewPartitioningSettingsQuery(
	tableName string,
	settings *PartitioningSettings,
	readReplicaSettings string,
) string {
	buf := make([]byte, 0, 64)
	buf = append(buf, "ALTER TABLE `"...)
	buf = helpers.AppendWithEscape(buf, tableName)
	buf = append(buf, '`', ' ')
	buf = append(buf, "SET (\n"...)

	// TODO(shmel1k@): remove copypaste.
	needComma := false
	if settings != nil && settings.ByLoad != nil {
		buf = append(buf, "AUTO_PARTITIONING_BY_LOAD = "...)
		val := *settings.ByLoad
		if val {
			buf = append(buf, "ENABLED"...)
		} else {
			buf = append(buf, "DISABLED"...)
		}
		needComma = true
	}
	if settings != nil && settings.BySize != nil {
		if needComma {
			buf = append(buf, ',', '\n')
		}
		needComma = true
		buf = append(buf, "AUTO_PARTITIONING_BY_SIZE = ENABLED"...)
		buf = append(buf, ',', '\n')
		buf = append(buf, "AUTO_PARTITIONING_PARTITION_SIZE_MB = "...)
		buf = strconv.AppendInt(buf, int64(*settings.BySize), 10)
	}
	if settings != nil && settings.MinPartitionsCount != 0 {
		if needComma {
			buf = append(buf, ',', '\n')
		}
		buf = append(buf, "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = "...)
		buf = strconv.AppendInt(buf, int64(settings.MinPartitionsCount), 10)
		needComma = true
	}
	if settings != nil && settings.MaxPartitionsCount != 0 {
		if needComma {
			buf = append(buf, ',', '\n')
		}
		buf = append(buf, "AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = "...)
		buf = strconv.AppendInt(buf, int64(settings.MaxPartitionsCount), 10)
	}
	if readReplicaSettings != "" {
		if needComma {
			buf = append(buf, ',', '\n')
		}
		buf = append(buf, "READ_REPLICAS_SETTINGS = \""...)
		buf = helpers.AppendWithEscape(buf, readReplicaSettings)
		buf = append(buf, '"')
	}
	buf = append(buf, '\n', ')')

	return string(buf)
}

//func prepareCDCAlterQuery(tableName string, cdc []*ChangeDataCaptureSettings) string {

//}

func PrepareAlterRequest(diff *tableDiff) string {
	if diff == nil {
		return ""
	}

	req := make([]byte, 0, defaultRequestCapacity)
	needSemiColon := false
	if len(diff.IndexToDrop) > 0 {
		needSemiColon = true
		for i, v := range diff.IndexToDrop {
			req = append(req, prepareDropIndexQuery(diff.TableName, v)...)
			if i != len(diff.IndexToDrop)-1 {
				req = append(req, ';', '\n')
			}
		}
	}
	if len(diff.ColumnsToAdd) > 0 {
		if needSemiColon {
			req = append(req, ';', '\n')
		}
		needSemiColon = true
		req = append(req, prepareAddColumnsQuery(diff.TableName, diff.ColumnsToAdd)...)
	}
	if len(diff.IndexToCreate) > 0 {
		if needSemiColon {
			req = append(req, ';', '\n')
		}
		needSemiColon = true
		for i, v := range diff.IndexToCreate {
			req = append(req, prepareAddIndexQuery(diff.TableName, v)...)
			if i != len(diff.IndexToCreate)-1 {
				req = append(req, ';', '\n')
			}
		}
	}
	if diff.NewTTLSettings != nil {
		if needSemiColon {
			req = append(req, ';', '\n')
		}
		needSemiColon = true
		req = append(req, prepareResetTTLQuery(diff.TableName)...)
		req = append(req, ';', '\n')
		req = append(req, prepareSetNewTTLSettingsQuery(diff.TableName, diff.NewTTLSettings)...)
	}
	if diff.NewPartitioningSettings != nil || diff.ReadReplicasSettings != "" {
		if needSemiColon {
			req = append(req, ';', '\n')
		}
		req = append(req, prepareNewPartitioningSettingsQuery(diff.TableName, diff.NewPartitioningSettings, diff.ReadReplicasSettings)...)
		needSemiColon = true
	}

	if diff.NewKeyBloomFilterSettings != nil {
		if needSemiColon {
			req = append(req, ';', '\n')
		}
		req = append(req, prepareKeyBloomFilterQuery(diff.TableName, *diff.NewKeyBloomFilterSettings)...)
		needSemiColon = true
	}

	_ = needSemiColon

	return string(req)
}

func PrepareDropTableRequest(tableName string) string {
	buf := make([]byte, 0, 64)
	buf = append(buf, "DROP TABLE `"...)
	buf = helpers.AppendWithEscape(buf, tableName)
	buf = append(buf, '`')
	return string(buf)
}
