package externaltable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeYdbContentScalar(t *testing.T) {
	assert.Equal(t, "csv_with_names", normalizeYdbContentScalar(`["csv_with_names"]`))
	assert.Equal(t, "gzip", normalizeYdbContentScalar(`["gzip"]`))
	assert.Equal(t, "parquet", normalizeYdbContentScalar("parquet"))
	assert.Equal(t, `["a","b"]`, normalizeYdbContentScalar(`["a","b"]`))
}
