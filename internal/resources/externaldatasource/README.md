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

- `auth_method` (String) - Authentication method. One of: `NONE`, `BASIC`, `MDB_BASIC`, `AWS`, `TOKEN`, `SERVICE_ACCOUNT`, `IAM`.
- `database_name` (String) - Database name in the external source. For Ydb IAM, use the full target database path.
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

## Ydb IAM with the caller token

Use `auth_method = "IAM"` with `source_type = "Ydb"` and the runtime
`service_account_id`. The provider uses the caller token from its authentication
configuration to bootstrap IAM delegation through a temporary secret. Configure
the caller token on the provider; IAM resources do not accept a bootstrap mode or
an initial token secret path.

```tf
resource "ydb_external_data_source" "cloud_target" {
  connection_string  = "grpcs://owner.example:2135/?database=/owner"
  path               = "cloud_target"
  source_type        = "Ydb"
  location           = "target.example:2135"
  database_name      = "/target/database/path"
  auth_method        = "IAM"
  service_account_id = "runtime-service-account-id"
  use_tls            = true
}
```

`database_name` identifies the target database by its full path. When `resource_id`
is omitted, YDB uses that path to resolve the target cloud ID. `database_id` does
not replace this path for IAM resolution. Set `resource_id` only when supplying an
explicit target cloud ID.

The owner YDB server must support IAM external data sources and have the feature
enabled. Configure delegation for the caller and target access for the runtime
service account separately. The provider does not provision databases or grant
cloud roles. The caller also needs permission to manage the EDS and its temporary
bootstrap secret in the owner database.

The temporary-secret DDL request contains the caller token as a quoted value.
The provider suppresses raw credential-bearing errors, and EDS resource state
does not store the token. Server-side query and audit logging follow the server's
configuration; the provider does not control or redact those logs.

### Cleanup and interrupted requests

After a confirmed EDS mutation, the provider removes its temporary secret. If
cleanup fails, a managed resource retains its identity and records the exact
owned secret in `pending_cleanup_path`. Refresh retries removal; run a normal
`terraform plan` to retry it. Update and destroy also retry pending cleanup before
changing the EDS. This attribute contains a path, never the caller token.

A lost mutation response does not prove that the server stopped using the secret.
For an existing resource, the provider records `pending_iam_mutation = "UNKNOWN"`
and blocks refresh, update and destroy. An unknown Create does not acquire an EDS
identity or persist recovery metadata; preserve the temporary secret path from
the diagnostic. Neither an equal EDS description nor an absent EDS proves that
the original request has terminated.

To recover an unknown request, or cleanup that cannot be retried successfully:

1. Back up Terraform state and preserve the resource ID and exact temporary secret
   path from state or the diagnostic.
2. Have the YDB operator establish that the relevant request has terminated.
   Inspect the EDS and reconcile its ownership, target and service account before
   importing it. If temporary-secret creation itself was unacknowledged, the EDS
   was not submitted; establish termination of that secret request instead.
3. After confirming ownership and termination, remove only that temporary secret
   from the owner database. Do not delete a secret based solely on its name prefix.
4. If the verified EDS exists, remove its old Terraform state entry when present
   and import its verified ID. Import clears the pending metadata. For an unknown
   Create, import only after independently establishing EDS ownership. If no EDS
   remains after the request has terminated, remove any stale state entry before
   creating it again.

For an existing resource, the state commands are:

```sh
terraform state pull > iam-state-backup.json
# Complete the server verification and owned-secret removal described above first.
terraform state rm ydb_external_data_source.cloud_target
terraform import ydb_external_data_source.cloud_target "$VERIFIED_EDS_ID"
terraform plan
```

### Live cloud acceptance test

`TestAccYdbExternalDataSource_iamCallerToken` creates a target table, seeds a row,
creates an IAM EDS, and reads that row through the EDS after bootstrap. It also
checks a no-op plan and import, then destroys the test resources. Both databases
and the required delegation/runtime grants must already exist; the owner database
must support federated queries to the target.

Set these variables explicitly:

- `YDB_ACC_CONNECTION_STRING`: owner database connection string.
- `YDB_ACC_TOKEN`: caller token, also used to create and seed the target fixture.
- `YDB_ACC_IAM_TARGET_CONNECTION_STRING`: target database connection string,
  including its full database path.
- `YDB_ACC_IAM_SERVICE_ACCOUNT_ID`: runtime service account with target read access.
- `YDB_ACC_IAM_FIXTURE_PREFIX`: dedicated relative namespace in both databases,
  such as `terraform_iam_acceptance`. Use letters, digits, underscores, hyphens
  and slashes; the test creates randomly named objects beneath it.

The test reads credentials from the environment without placing them in its HCL.
Enable the dedicated gate and run only this test:

```sh
TF_ACC=1 YDB_ACC_IAM_LIVE=1 go test -v ./internal/terraform/ \
  -run '^TestAccYdbExternalDataSource_iamCallerToken$' -timeout 15m
```

With the gate enabled, missing fixture configuration fails the test. Ordinary
test runs skip it. This real-cloud test has not yet been run for this change;
local tests do not establish live delegation or runtime authorization. The
separate `TestAccIAMSecret` acceptance test checks the actual temporary-secret
DDL against local YDB; CI runs it against both `latest` and `edge` images.
