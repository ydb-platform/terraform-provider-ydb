package externaldatasource

import (
	"strings"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

type withParam struct {
	Key   string
	Value string
}

func collectWithParams(r *Resource) []withParam {
	params := make([]withParam, 0, len(allStringAttrKeys)+1)
	for _, k := range allStringAttrKeys {
		v := r.strAttr(k)
		if v == "" && k != "source_type" && k != "location" {
			continue
		}
		params = append(params, withParam{Key: strings.ToUpper(k), Value: v})
	}
	if r.UseTLS != nil {
		v := "FALSE"
		if *r.UseTLS {
			v = "TRUE"
		}
		params = append(params, withParam{Key: "USE_TLS", Value: v})
	}
	return params
}

func appendWithClause(buf []byte, params []withParam) []byte {
	buf = append(buf, " WITH ("...)
	for i, p := range params {
		buf = append(buf, ' ')
		buf = append(buf, p.Key...)
		buf = append(buf, " = \""...)
		buf = append(buf, escapeYQLDoubleString(p.Value)...)
		buf = append(buf, '"')
		if i < len(params)-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, " )"...)
	return buf
}

func PrepareDataSourceQuery(fullPath string, r *Resource) string {
	return prepareDataSourceQuery(fullPath, r, "CREATE OR REPLACE")
}

func prepareDataSourceQuery(fullPath string, r *Resource, operation string) string {
	buf := make([]byte, 0, 512)
	buf = append(buf, operation...)
	buf = append(buf, " EXTERNAL DATA SOURCE `"...)
	buf = append(buf, helpers.EscapeYQLIdentifier(fullPath)...)
	buf = append(buf, '`')
	buf = appendWithClause(buf, collectWithParams(r))
	return string(buf)
}

func PrepareDropQuery(fullPath string) string {
	buf := make([]byte, 0, 128)
	buf = append(buf, "DROP EXTERNAL DATA SOURCE `"...)
	buf = append(buf, helpers.EscapeYQLIdentifier(fullPath)...)
	buf = append(buf, '`')
	return string(buf)
}

func escapeYQLDoubleString(s string) string {
	return strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n", "\r", "\\r", "\t", "\\t").Replace(s)
}

// IAM Create must not replace an existing EDS, even if its description matches.
func PrepareCreateDataSourceQuery(fullPath string, r *Resource) string {
	if r.strAttr("auth_method") == "IAM" {
		return prepareDataSourceQuery(fullPath, r, "CREATE")
	}
	return PrepareDataSourceQuery(fullPath, r)
}
