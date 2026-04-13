# ydb_external_table resource

`ydb_external_table` resource is used to manage YDB external table entities. External tables describe data stored in external sources (e.g. S3-compatible storage) and allow reading and writing via standard SQL.

Since YDB does not support `ALTER EXTERNAL TABLE`, any attribute change triggers a full recreation of the resource.

## Example

```tf
resource "ydb_external_table" "s3_data" {
    connection_string = "grpc://localhost:2136/?database=/local"
    path              = "s3_test_data"
    data_source_path  = "bucket"
    location          = "folder"
    format            = "csv_with_names"
    compression       = "gzip"

    column {
        name     = "key"
        type     = "Utf8"
        not_null = true
    }
    column {
        name     = "value"
        type     = "Utf8"
        not_null = true
    }
}
```

## Argument Reference

* `connection_string` - (Required) Database connection string.
* `path` - (Required) Path to the external table within the database.
* `data_source_path` - (Required) Name of the external data source (created via `ydb_external_data_source`).
* `location` - (Required) Path within the external data source (e.g. folder in S3 bucket).
* `format` - (Required) Data format. Valid values: `csv_with_names`, `tsv_with_names`, `json_list`, `json_each_row`, `parquet`, `raw`.
* `compression` - (Optional) Compression algorithm (e.g. `gzip`).
* `column` - (Required) One or more column definitions. Each block supports:
    * `name` - (Required) Column name.
    * `type` - (Required) Column data type (YQL type, e.g. `Utf8`, `Int64`, `Double`).
    * `not_null` - (Optional) Whether the column is non-nullable. Default: `false`.

## Data Source

The `ydb_external_table` data source reads an existing external table.

```tf
data "ydb_external_table" "example" {
    connection_string = "grpc://localhost:2136/?database=/local"
    path              = "s3_test_data"
}
```

### Data Source Attributes

All arguments from the resource are exported as read-only attributes: `column`, `data_source_path`, `location`, `format`, `compression`.
