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

`database_name` identifies the target database by its full path. YDB resolves the
target cloud ID from that path for every mutation. `database_id` does not replace
this path for IAM resolution.

The owner YDB server must support IAM external data sources and have the feature
enabled. Configure delegation for the caller and target access for the runtime
service account separately. The provider does not provision databases or grant
cloud roles. The caller also needs permission to manage the EDS and its temporary
bootstrap secret in the owner database.

The caller token is sent as a query parameter, never as a SQL literal, and is not
stored in EDS resource state. This requires a server supporting parameterized
secret DDL, verified on YDB 26.3.1.17. YDB 26.2.1.14 rejects it; the provider does
not fall back to embedding the token in SQL. YDB also has an
[upstream regression test for parameterized secret values in server TRACE logs](https://github.com/ydb-platform/ydb/blob/512da6c36db5f25306733be23672464aec73a642/ydb/tests/functional/secrets/slow/test_secrets_logging.py#L54-L86).

SDK logging remains available. IAM server failures report status codes instead of
raw error text; authentication callback errors retain their original diagnostic
text. Terraform's `TF_LOG_SDK_PROTO_DATA_DIR` writes raw protocol payloads; these
contain the caller token when supplied in provider configuration, independently
of secret creation.

### Cleanup and interrupted requests

After a confirmed EDS mutation, the provider removes its temporary secret. If
cleanup fails, the managed resource retains its identity and a warning reports
the exact owned secret path for manual removal. Preserve that path from the
diagnostic. The provider does not store recovery metadata or retry cleanup on
later invocations.

A lost mutation response does not prove that the server stopped using the secret.
The provider retains it and reports the exact path. Have the YDB operator confirm
that the original request has terminated before removing that secret or applying
another mutation. Later Terraform operations are not automatically blocked.
Neither an equal EDS description nor an absent EDS proves request termination.
An unacknowledged Create does not claim ownership; import an existing EDS only
after independently verifying its ownership, target and service account. If the
temporary-secret Create was unacknowledged, the EDS request was not submitted.

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

With the gate enabled, missing fixture configuration fails the test. The existing
GitHub Actions workflow starts local YDB containers and does not configure cloud
fixtures or set this gate, so this scenario is skipped in CI. It has not yet been
run for this change; local tests do not establish live delegation or runtime
authorization.

All IAM acceptance tests are in `internal/terraform` and run with the existing
acceptance suite under `TF_ACC=1`. Terraform CLI scenarios cover lifecycle and
failure diagnostics against a controlled gRPC server. The local YDB test
`TestAccYdbExternalDataSource_iamBootstrapCleanup` checks real temporary-secret
DDL and cleanup after a rejected EDS Create on servers supporting secret
parameters. Older servers must reject bootstrap without a literal fallback.
Both cases preserve the existing EDS.
The existing CI acceptance job runs these tests against `latest` and `edge`.
