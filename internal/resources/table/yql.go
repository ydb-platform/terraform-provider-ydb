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

func PrepareCreateRequest(r *TableResource) string {
	req := make([]byte, 0, defaultRequestCapacity)

	req = append(req, "CREATE TABLE `"...)
	req = append(req, r.Path...)
	req = append(req, "`("...)
	req = append(req, '\n')

	indent := 0
	indent++
	for _, v := range r.Columns {
		req = appendIndent(req, indent)
		req = append(req, v.Name...) // TODO(shmel1k@): escape
		req = append(req, ' ')
		req = append(req, v.Type...) // TODO(shmel1k@): escape
		if v.Family != "" {
			req = append(req, ' ')
			req = append(req, "FAMILY "...)
			req = append(req, '`')
			req = append(req, v.Family...)
			req = append(req, '`')
		}
		req = append(req, ',')
		req = append(req, '\n')
	}

	for _, v := range r.Indexes {
		req = appendIndent(req, indent)
		req = append(req, "INDEX"...)
		req = append(req, ' ')
		req = append(req, '`')
		req = append(req, v.Name...)
		req = append(req, '`')
		req = append(req, ' ')
		req = append(req, "GLOBAL"...)
		if v.Type == "global_async" { // TODO(shmel1k@): to consts
			req = append(req, " ASYNC"...)
		} else {
			req = append(req, " SYNC"...)
		}
		req = append(req, ' ')
		req = append(req, "ON"...)
		req = append(req, ' ')
		req = append(req, '(')
		for _, c := range v.Columns {
			req = append(req, '`')
			req = append(req, c...)
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
				req = append(req, c...)
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

	/*
				CREATE TABLE my_table (
				    a Uint64,
				    b Bool,
				    c Uft8,
				    d Date,
				INDEX idx_d GLOBAL ON (d),
				INDEX idx_ba GLOBAL ASYNC ON (b, a) COVER (c),
			    PRIMARY KEY (a),
		        FAMILY default (
		            DATA = "ssd",
		            COMPRESSION = "off"
		        ),
		        FAMILY family_large (
		            DATA = "hdd",
		            COMPRESSION = "lz4"
		        ))
				WITH (
				AUTO_PARTITIONING_BY_SIZE = ENABLED,
				AUTO_PARTITIONING_PARTITION_SIZE_MB = 512
				);
	*/

	req = appendIndent(req, indent)
	req = append(req, "PRIMARY KEY"...)
	req = append(req, ' ')
	req = append(req, '(')
	for _, v := range r.PrimaryKey.Columns {
		req = append(req, '`')
		req = append(req, v...)
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
			req = append(req, v.Name...)
			req = append(req, '`')
			req = append(req, '(')
			req = append(req, '\n')
			indent++
			req = appendIndent(req, indent)
			req = append(req, "DATA = "...)
			req = append(req, '"')
			req = append(req, v.Data...) // XXX
			req = append(req, '"')
			req = append(req, ',')
			req = append(req, '\n')
			req = appendIndent(req, indent)
			req = append(req, "COMPRESSION = "...) // XXX
			req = append(req, '"')
			req = append(req, v.Compression...) // XXX
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
	if r.AutoPartitioning != nil {
		needWith = true
	}
	if r.PartitioningPolicy != nil {
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
		req = append(req, r.TTL.Interval...) // XXX(shmel1k@): escape
		req = append(req, '"')
		req = append(req, ')')
		req = append(req, " ON "...)
		req = append(req, '`')
		req = append(req, r.TTL.ColumnName...)
		req = append(req, '`')
		req = append(req, '\n')
		needComma = true
	}
	if r.AutoPartitioning != nil {
		if r.AutoPartitioning.ByLoad != nil {
			if needComma {
				req = append(req, ',')
			}
			req = appendIndent(req, indent)
			if *r.AutoPartitioning.ByLoad {
				req = append(req, "AUTO_PARTITIONING_BY_LOAD = ENABLED"...)
			} else {
				req = append(req, "AUTO_PARTITIONING_BY_LOAD = DISABLED"...)
			}
			req = append(req, ',')
			req = append(req, '\n')
			needComma = true
		}
		if r.AutoPartitioning.BySize != nil {
			if needComma {
				req = append(req, ',')
			}
			req = appendIndent(req, indent)
			req = append(req, "AUTO_PARTITIONING_BY_SIZE_ENABLED = ENABLED"...)
			req = append(req, ',')
			req = append(req, '\n')
			req = appendIndent(req, indent)
			req = append(req, "AUTO_PARTITIONING_BY_SIZE = "...)
			req = strconv.AppendInt(req, int64(*r.AutoPartitioning.BySize), 10)
			req = append(req, ',')
			req = append(req, '\n')
			needComma = true
		}
	}
	if r.PartitioningPolicy != nil {
	}
	indent--

	req = append(req, ')')

	return string(req)
}
