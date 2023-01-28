package table

import (
	"fmt"
	"strconv"
)

func isIntColumn(typ string) bool {
	return typ == "Int8" || typ == "Int16" || typ == "Int32" || typ == "Int64"
}

func isUintColumn(typ string) bool {
	return typ == "Uint8" || typ == "Uint16" || typ == "Uint32" || typ == "Uint64"
}

func isBoolColumn(typ string) bool {
	return typ == "Bool"
}

func isFloatColumn(typ string) bool {
	return typ == "Float" || typ == "Decimal" || typ == "Double"
}

func isStringColumn(typ string) bool {
	return typ == "Utf8" || typ == "Bytes"
}

func parsePartitionKey(k string, typ string) (interface{}, error) {
	if isIntColumn(typ) {
		return strconv.ParseInt(k, 10, 64)
	}
	if isUintColumn(typ) {
		return strconv.ParseUint(k, 10, 64)
	}
	if isFloatColumn(typ) {
		return strconv.ParseFloat(k, 64)
	}
	if isStringColumn(typ) {
		return k, nil
	}
	if isBoolColumn(typ) {
		return strconv.ParseBool(k)
	}
	return nil, fmt.Errorf("unknown column type %q", typ)
}
