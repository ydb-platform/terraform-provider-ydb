package table

import (
	"bytes"
	"strconv"
)

const (
	defaultRequestCapacity = 1024 // 1 KiB
)

func appendIndent(req []byte, indent int) []byte {
	req = append(req, bytes.Repeat([]byte{'\t'}, indent)...)
	return req
}

func appendWithEscape(buf []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		if s[i] == '"' || s[i] == '/' {
			buf = append(buf, '\\')
		}
		buf = append(buf, s[i])
	}
	return buf
}

func PrepareCreateRequest(r *Resource) string { //nolint:gocyclo
	req := make([]byte, 0, defaultRequestCapacity)

	req = append(req, "CREATE TABLE `"...)
	req = appendWithEscape(req, r.FullPath)
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
		req = appendWithEscape(req, v.Name)
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
			req = appendWithEscape(req, c)
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
				req = appendWithEscape(req, c)
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
		req = appendWithEscape(req, v)
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
			req = appendWithEscape(req, v.Name)
			req = append(req, '`')
			req = append(req, '(')
			req = append(req, '\n')
			indent++
			req = appendIndent(req, indent)
			req = append(req, "DATA = "...)
			req = append(req, '"')
			req = appendWithEscape(req, v.Data)
			req = append(req, '"')
			req = append(req, ',')
			req = append(req, '\n')
			req = appendIndent(req, indent)
			req = append(req, "COMPRESSION = "...)
			req = append(req, '"')
			req = appendWithEscape(req, v.Compression)
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
		req = appendWithEscape(req, r.TTL.ExpireInterval)
		req = append(req, '"')
		req = append(req, ')')
		req = append(req, " ON "...)
		req = append(req, '`')
		req = appendWithEscape(req, r.TTL.ColumnName)
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
			req = append(req, "AUTO_PARTITIONING_BY_SIZE_ENABLED = ENABLED"...)
			req = append(req, ',')
			req = append(req, '\n')
			req = appendIndent(req, indent)
			req = append(req, "AUTO_PARTITIONING_BY_SIZE = "...)
			req = strconv.AppendInt(req, int64(*r.PartitioningSettings.BySize), 10)
			req = append(req, ',')
			req = append(req, '\n')
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
						req = appendWithEscape(req, t)
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
		req = appendWithEscape(req, r.ReplicationSettings.ReadReplicasSettings)
		req = append(req, '"')
		//			needComma = true
	}
	// indent--

	// TODO(shmel1k@): add KEY_BLOOM_FILTER
	req = append(req, '\n', ')')

	return string(req)
}

func prepareDropIndexQuery(tableName string, indexToDrop string) string {
	req := []byte("ALTER TABLE `")
	req = appendWithEscape(req, tableName)
	req = append(req, '`', ' ')
	req = append(req, "DROP INDEX `"...)
	req = appendWithEscape(req, indexToDrop)
	req = append(req, '`')
	return string(req)
}

func prepareAddIndexQuery(tableName string, indexToAdd *Index) string {
	req := []byte("ALTER TABLE `")
	req = appendWithEscape(req, tableName)
	req = append(req, '`', ' ')
	req = append(req, "ADD INDEX `"...)
	req = appendWithEscape(req, indexToAdd.Name)
	req = append(req, '`', ' ')
	// TODO(shmel1k@): add ToYQL for index
	if indexToAdd.Type == "global_async" { // TODO(shmel1k@): move to consts
		req = append(req, "GLOBAL ASYNC ON ("...)
	} else {
		req = append(req, "GLOBAL SYNC ON ("...)
	}
	for i := 0; i < len(indexToAdd.Columns); i++ {
		req = append(req, '`')
		req = appendWithEscape(req, indexToAdd.Columns[i])
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
			req = appendWithEscape(req, indexToAdd.Cover[i])
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
	req = appendWithEscape(req, tableName)
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
	req = appendWithEscape(req, tableName)
	req = append(req, '`', ' ')
	for i := 0; i < len(columnsToDrop); i++ {
		req = append(req, "DROP COLUMN `"...)
		req = appendWithEscape(req, columnsToDrop[i])
		req = append(req, '`')
		if i != len(columnsToDrop)-1 {
			req = append(req, ',', ' ')
		}
	}

	return string(req)
}

func PrepareAlterRequest(diff *tableDiff) (string, error) {
	req := make([]byte, 0, defaultRequestCapacity)
	if len(diff.IndexToDrop) > 0 {

	}

	return string(req), nil
}
