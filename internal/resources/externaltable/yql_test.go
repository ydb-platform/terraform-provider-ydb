package externaltable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrepareCreateQuery(t *testing.T) {
	tests := []struct {
		name     string
		fullPath string
		resource *Resource
		want     string
	}{
		{
			name:     "basic with all fields",
			fullPath: "/local/s3_test_data",
			resource: &Resource{
				DataSourcePath: "bucket",
				Location:       "folder",
				Format:         "csv_with_names",
				Compression:    "gzip",
				Columns: []ColumnDef{
					{Name: "key", Type: "Utf8", NotNull: true},
					{Name: "value", Type: "Utf8", NotNull: true},
				},
			},
			want: `CREATE EXTERNAL TABLE ` + "`/local/s3_test_data`" + ` ( ` + "`key`" + ` Utf8 NOT NULL, ` + "`value`" + ` Utf8 NOT NULL ) WITH ( DATA_SOURCE = "bucket", LOCATION = "folder", FORMAT = "csv_with_names", COMPRESSION = "gzip" )`,
		},
		{
			name:     "without compression",
			fullPath: "/local/my_table",
			resource: &Resource{
				DataSourcePath: "my_source",
				Location:       "data/",
				Format:         "parquet",
				Columns: []ColumnDef{
					{Name: "id", Type: "Int64", NotNull: true},
					{Name: "name", Type: "Utf8", NotNull: false},
				},
			},
			want: `CREATE EXTERNAL TABLE ` + "`/local/my_table`" + ` ( ` + "`id`" + ` Int64 NOT NULL, ` + "`name`" + ` Utf8 ) WITH ( DATA_SOURCE = "my_source", LOCATION = "data/", FORMAT = "parquet" )`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PrepareCreateQuery(tt.fullPath, tt.resource)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPrepareDropQuery(t *testing.T) {
	got := PrepareDropQuery("/local/s3_test_data")
	assert.Equal(t, "DROP EXTERNAL TABLE `/local/s3_test_data`", got)
}
