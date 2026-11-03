# ydb_external_data_source resource

`ydb_external_data_source` resource is used to manage YDB [external data source](https://ydb.tech/docs/ru/concepts/datamodel/external_data_source) entity.

Configuration changes are applied in place via `CREATE OR REPLACE EXTERNAL DATA SOURCE`. Only `connection_string`, `path`, and `source_type` require resource recreation.

## Example

```tf
resource "ydb_external_data_source" "object_storage" {
    path              = "path/to/external_data_source"
    connection_string = "grpc://localhost:2136/?database=/local"

    source_type = "ObjectStorage"
    location    = "https://storage.yandexcloud.net/my_bucket_name/"
    auth_method = "NONE"
}
```

```tf
resource "ydb_external_data_source" "clickhouse" {
    path              = "path/to/clickhouse_source"
    connection_string = "grpc://localhost:2136/?database=/local"

    source_type          = "ClickHouse"
    location             = "clickhouse-host:9000"
    auth_method          = "BASIC"
    login                = "user"
    password_secret_name = "my_password_secret"
    database_name        = "default"
    protocol             = "NATIVE"
    use_tls              = true
}
```

```tf
resource "ydb_external_data_source" "postgresql" {
    path              = "path/to/pg_source"
    connection_string = "grpc://localhost:2136/?database=/local"

    source_type                  = "PostgreSQL"
    location                     = "localhost:6432"
    auth_method                  = "MDB_BASIC"
    service_account_id           = "sa-id-123"
    service_account_secret_path  = "sa_secret"
    login                        = "pguser"
    password_secret_path         = "pg_pass"
    database_name                = "mydb"
    mdb_cluster_id               = "c9q1234567890"
    use_tls                      = true
}
```

```tf
resource "ydb_external_data_source" "s3_aws" {
    path              = "path/to/s3_source"
    connection_string = "grpc://localhost:2136/?database=/local"

    source_type                        = "ObjectStorage"
    location                           = "https://storage.yandexcloud.net/my_bucket_name/"
    auth_method                        = "AWS"
    aws_access_key_id_secret_path      = "aws_key_id"
    aws_secret_access_key_secret_path  = "aws_secret_key"
    aws_region                         = "ru-central-1"
}
```

## Argument Reference

### Required

- `connection_string` (String) - Database connection string, e.g. `grpc://localhost:2136/?database=/local`.
- `path` (String) - Path to the external data source within the database.
- `source_type` (String) - Type of external data source: `ObjectStorage`, `ClickHouse`, `PostgreSQL`.
- `location` (String) - Network address of the external data source (host:port or URL).

### Optional

- `auth_method` (String) - Authentication method. One of: `NONE`, `BASIC`, `MDB_BASIC`, `AWS`, `TOKEN`, `SERVICE_ACCOUNT`.
- `database_name` (String) - Database name in the external source (for ClickHouse/PostgreSQL).
- `protocol` (String) - Communication protocol: `NATIVE`, `HTTP`.
- `use_tls` (Boolean) - Enable TLS for the external data source connection.
- `mdb_cluster_id` (String) - Managed Database cluster ID.

#### BASIC / MDB_BASIC auth parameters

- `login` (String) - Username.
- `password_secret_name` (String) - Secret name for the password.
- `password_secret_path` (String) - Secret path for the password.

> Specify either `password_secret_name` or `password_secret_path`, not both.

#### MDB_BASIC additional parameters

- `service_account_id` (String) - Service account ID.
- `service_account_secret_name` (String) - Secret name for the service account.
- `service_account_secret_path` (String) - Secret path for the service account.

#### SERVICE_ACCOUNT auth parameters

- `service_account_id` (String) - Service account ID.
- `service_account_secret_name` (String) - Secret name for the service account.
- `service_account_secret_path` (String) - Secret path for the service account.

#### AWS auth parameters

- `aws_access_key_id_secret_path` (String) - Secret path for AWS access key ID.
- `aws_secret_access_key_secret_path` (String) - Secret path for AWS secret access key.
- `aws_region` (String) - AWS region.

#### TOKEN auth parameters

- `token_secret_path` (String) - Secret path for the token.

## Validation

The provider validates `auth_method` parameters before sending requests to YDB:

- **Unsupported parameters** - fields belonging to a different auth method are rejected (e.g. `aws_region` with `AUTH_METHOD = "BASIC"`).
- **Required parameters** - mandatory fields for each auth method must be provided (e.g. `login` for `BASIC`).

## Data Source

`ydb_external_data_source` data source reads an existing external data source. Only `connection_string` and `path` are required; all other attributes are computed.

```tf
data "ydb_external_data_source" "example" {
    path              = "path/to/external_data_source"
    connection_string = "grpc://localhost:2136/?database=/local"
}
```

## Cloud IAM using an existing secret

Use `auth_method = "IAM"` with `source_type = "Ydb"`, a runtime
`service_account_id`, and a configured `initial_token_secret_path` naming an
existing secret. The caller must have the required delegation and target access.
Configure target delegation and runtime roles separately.

```tf
resource "ydb_external_data_source" "cloud_target" {
    connection_string         = "grpcs://owner.example:2135/?database=/owner"
    path                      = "cloud_target"
    source_type               = "Ydb"
    location                  = "target.example:2135"
    database_id               = "target-database-id"
    auth_method               = "IAM"
    service_account_id        = "runtime-service-account-id"
    initial_token_secret_path = "secrets/initial-token"
    use_tls                   = true
}
```

IAM defaults to `iam_bootstrap_mode = "EXISTING_SECRET"` and persists this mode
in state. The secret remains externally owned: create, update, import, read and
destroy never create, alter or delete secrets. Equivalent relative and absolute
owner paths compare equally. `resource_id` is the target cloud ID; normally omit
it so YDB resolves it. Changing the target invalidates an implicitly resolved ID.

IAM Create uses strict `CREATE EXTERNAL DATA SOURCE`, preserving foreign objects
on collision. Update uses `CREATE OR REPLACE` in place, preserving the EDS identity
and dependent tables. An acknowledged IAM mutation retains its identity with a
warning if read-back fails. Credential error details are omitted from diagnostics.

Import and Read accept observed historical secret paths even if their secrets no
longer exist, and default imported IAM state to `EXISTING_SECRET`. Configuration
must supply a usable secret path before planning or applying an update.

A lost Update response can leave the EDS request running. The provider retains its
ID and `pending_iam_mutation = "UNKNOWN"`, blocking refresh and further mutations.
Neither an equal description nor absence establishes termination. Back up state,
verify with the YDB operator that the request has terminated, reconcile ownership,
and re-import the verified EDS to clear uncertain metadata. An unknown Create does
not invent ownership; import only after independently verifying it.

New IAM state requires a provider containing this schema. Preserve backups before
rollback and intentionally transfer management of IAM resources before using an
older provider. The local Terraform lifecycle test exercises create, no-op plans,
in-place updates, import, and destroy against fake gRPC; it verifies that the
external secret survives. It does not establish live cloud authorization or server
upgrade/downgrade behavior. This feature changes no server protocol or persisted
server format.
