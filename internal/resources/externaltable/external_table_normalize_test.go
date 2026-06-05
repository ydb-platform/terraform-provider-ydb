package externaltable

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestNormalizeYdbContentScalar(t *testing.T) {
	assert.Equal(t, "csv_with_names", normalizeYdbContentScalar(`["csv_with_names"]`))
	assert.Equal(t, "gzip", normalizeYdbContentScalar(`["gzip"]`))
	assert.Equal(t, "parquet", normalizeYdbContentScalar("parquet"))
	assert.Equal(t, `["a","b"]`, normalizeYdbContentScalar(`["a","b"]`))
}

func TestUnwrapTypeNormalizesCase(t *testing.T) {
	typ, notNull := unwrapType(types.TypeUTF8)
	assert.Equal(t, "utf8", typ)
	assert.True(t, notNull)

	typ, notNull = unwrapType(types.Optional(types.TypeInt32))
	assert.Equal(t, "int32", typ)
	assert.False(t, notNull)
}
