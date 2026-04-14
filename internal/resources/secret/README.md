# ydb_secret resource

`ydb_secret` resource is used to manage YDB secret objects.

## Example

### Static value

```tf
resource "ydb_secret" "secret" {
    connection_string   = "grpc://localhost:2136/?database=/local"
    name                = "my_secret"
    value               = "s3cr3t_v4lue"
    inherit_permissions = true
}
```

### Value from command

```tf
resource "ydb_secret" "from_script" {
    connection_string = "grpc://localhost:2136/?database=/local"
    name              = "my_secret"

    command {
        path = "/usr/bin/bash"
        args = ["-c", "cat /run/secrets/db_password"]
    }
}
```

## Argument Reference

- `connection_string` (Required) - Connection string for YDB database.
- `name` (Required) - Secret name.
- `value` (Optional, Sensitive) - Secret value. Stored as a scrypt hash in Terraform state. Mutually exclusive with `command`.
- `command` (Optional) - Command to execute to generate the secret value. The command's stdout is used as the value. Mutually exclusive with `value`.
  - `path` (Required) - Path to the executable.
  - `args` (Optional) - List of arguments to pass to the command.
  - `env` (Optional) - Map of environment variables to set for the command.
- `inherit_permissions` (Optional, Default: `false`) - If `true`, the secret is created with access rights inherited from its parent directory. If `false`, only `DESCRIBE SCHEMA` permission is inherited. YDB does not return this setting in Describe, so the provider cannot read it back; Terraform keeps the value from your configuration in state, not from the server.
