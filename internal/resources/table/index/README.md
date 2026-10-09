# ydb_table_index resource

## Example

```tf
resource "ydb_table_index" "index" {
    table_path        = "path/to/table"
    connection_string = "grpc://localhost:2136/?database=/local"
    name              = "my_index"
    type              = "global_sync"
    columns           = ["a", "b"]
    cover             = ["c"]
}
```

`type` is optional. Omit it or set `type = ""` to use YDB's default synchronous
global index (`GLOBAL ON`). The provider reports its actual `global_sync` type
in Terraform state without planning a change for an omitted or empty `type`.
Set `type = "global_unique_index"` to enforce
uniqueness across the indexed columns. YDB creates a `GLOBAL UNIQUE SYNC` index.
Changing the index type recreates the resource. To add a unique index to an
existing table, the YDB cluster must enable `enable_add_unique_index`.

## Migrating an invalid `type` value

Older provider versions accepted any nonempty `type` value. Only `global_async`
created an asynchronous index; other values, including typos, created a
synchronous index. The provider read the actual index type back as `global_sync`
into Terraform state.

If your configuration contains a typo, change `type` to `global_sync` and run a
normal `terraform plan` with refresh enabled. The index should not be recreated.
If you intended an asynchronous index, change `type` to `global_async` instead;
Terraform will replace the existing synchronous index. No manual state edit is
needed for state written by the older provider.
