# ydb_secret resource

`ydb_secret` resource is used to manage YDB secret objects.

## Example

```tf
resource "ydb_secret" "secret" {
    connection_string   = "grpc://localhost:2136/?database=/local"
    name                = "my_secret"
    value               = "s3cr3t_v4lue"
    inherit_permissions = true
}
```

## Argument Reference

- `connection_string` (Required) - Connection string for YDB database.
- `name` (Required) - Secret name.
- `value` (Required, Sensitive) - Secret value.
- `inherit_permissions` (Optional, Default: `false`) - If `true`, the secret inherits access rights from its parent directory. If `false`, only `DESCRIBE SCHEMA` permission is inherited.
