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

## Cloud IAM for a YDB target

Use `auth_method = "IAM"` with `source_type = "Ydb"` and the runtime
`service_account_id`. The provider caller must already have permission to use
that account, resolve the target, and create secrets and EDS objects in the owner
database. Configure the target database's delegation and runtime roles separately.
The caller token bootstraps the operation; it is not the runtime SA credential.

```tf
resource "ydb_external_data_source" "cloud_target" {
    connection_string  = "grpcs://streaming.example:2135/?database=/streaming"
    path               = "cloud_target"
    source_type        = "Ydb"
    location           = "target.example:2135"
    database_id        = "target-database-id"
    auth_method        = "IAM"
    service_account_id = "runtime-service-account-id"
    iam_bootstrap_mode = "CALLER_TOKEN"
    use_tls            = true
}
```

`connection_string` names the owner/streaming database; `database_id`,
`database_name`, and `location` identify the target. Supply a target endpoint
reachable by the YDB runtime (including the appropriate `u-lb` endpoint in
installations that require it). The generic provider does not rewrite endpoints.
`resource_id` is Optional+Computed and means the **target cloud ID**, not either
database ID. Usually omit it so YDB resolves it. Changing the target invalidates
an implicitly resolved cloud ID; an explicitly configured ID remains authoritative.

New IAM resources default to `CALLER_TOKEN`. On every Create/Update, the provider
gets fresh caller credentials from its existing callback, strictly creates a
random root secret, passes its path to ordinary EDS DDL, then awaits secret
removal. The value is a Utf8 `$value` query parameter and is never placed in EDS
resource state, SQL literals, or diagnostics. A fixed provider token still expires:
credential renewal is the caller callback's responsibility. Go strings do not
provide a secure memory-zeroization guarantee.

`initial_token_secret_path` is Optional+Computed. In `CALLER_TOKEN` mode, omit it
from configuration: its state value is Describe metadata for a temporary object
that is normally already absent. Refresh and a no-op plan neither recreate that
secret nor request a new bootstrap token. Reads may authenticate their ordinary
YDB requests with the provider callback.

Use `iam_bootstrap_mode = "EXISTING_SECRET"` and a configured
`initial_token_secret_path` to use an externally managed legacy secret. The
provider never alters or deletes it. Relative paths and their equivalent absolute
owner paths compare equally. Import defaults safely to `EXISTING_SECRET` and
reads successfully even if the observed initial secret was removed. Before an
update, either explicitly select `CALLER_TOKEN` (and omit the path), or configure
a usable external secret path; copying an observed temporary path is insufficient.

IAM Create uses strict `CREATE EXTERNAL DATA SOURCE`; it never replaces an
existing object. IAM Update uses `CREATE OR REPLACE` without a preliminary DROP,
preserving dependent external tables. Existing non-IAM Create behavior remains
`CREATE OR REPLACE`.

### Cleanup and interrupted operations

Cleanup obtains fresh transport credentials and uses a separate ten-second context
after terminal EDS success or failure, including caller cancellation. It removes
only acknowledged, owned secrets. A collision gets a new random name without
altering the colliding object. After failed DROP, only a successful directory
listing confirming exact-path absence counts as successful cleanup.

After successful EDS mutation, failed cleanup returns a **warning** and persists
`pending_cleanup_path`, retaining the confirmed EDS without taint: Terraform 1.5.7
would taint a Create that returned an error and replace it on the next apply.
Refresh retries that exact secret; persistent failure retains identity/metadata
and blocks planning/apply. Restore secret DDL permissions or connectivity, then run
`terraform apply` or `terraform apply -refresh-only`. Keep pending metadata intact.
Failed read-back after acknowledged IAM mutation also retains identity with a warning.

A deadline or transport loss can leave execution unknown. Neither an equal Describe
nor absence proves Create ownership or termination. Unacknowledged secret CREATE
paths are reported as **unconfirmed** and never automatically dropped; acknowledged
temporary secrets remain while EDS mutation may run. Unknown Update keeps its ID
and `pending_iam_mutation = "UNKNOWN"`; CALLER_TOKEN also keeps `pending_cleanup_path`.
Refresh and further mutations stay blocked pending operator recovery. Unknown/failed
Create never invents an EDS identity to retain cleanup state.

Before manual recovery, back up state and verify with the YDB operator that the EDS
request has terminated. Drop only exact **confirmed-owned orphan** paths (using
proper identifier quoting), verify EDS ownership, and import if appropriate. For an
unknown Update, finish this recovery and re-import the verified EDS to clear uncertain
metadata, then restore the intended bootstrap mode. Unconfirmed paths require
independent ownership evidence; never sweep by `__tf_iam_bootstrap_` prefix.

A hard process kill before Terraform receives state can leave an uncheckpointed
orphan. Reruns use new names and cannot discover that orphan's ownership. Recover
using server audit records and independent ownership evidence. Token expiry limits
credential life but does not remove the secret object. Back up state before rollback.

### Provider compatibility and live verification

This feature adds provider fields and no YDB protocol/server changes. New IAM
configuration requires a provider release containing this schema. Before downgrading
providers, preserve backups and intentionally remove or transfer management of new
IAM resources; restore compatible configuration and state. An old provider is not
expected to accept arbitrary new IAM state. Legacy-only old→new→old state is
covered by the local `TestLocalTerraformYDBLegacyProviderUpgradeDowngrade` when
an explicit baseline binary is supplied. This changes only Go providers using
existing SQL/CP APIs: no YDB or YDB CP upgrade, rolling deployment, mixed-version
server exercise or server restart is required to deploy this feature.

The local tests exercise real Terraform 1.5.7 state/plan semantics, real SDK query
parameter wiring against fake gRPC, cancellation, unknown replies, and process kill.
They do not establish live cloud authorization or runtime behavior after secret DROP.
`TestAccYDBExternalDataSourceIAM` is a separately gated live DDL/no-op/import test:
set `TF_ACC=1`, `YDB_ACC_IAM_LIVE=1`, `YDB_ACC_CONNECTION_STRING`, `YDB_ACC_TOKEN`,
`YDB_ACC_IAM_TARGET_DATABASE_ID`, `YDB_ACC_IAM_SERVICE_ACCOUNT_ID`,
`YDB_ACC_IAM_TARGET_LOCATION`, and `YDB_ACC_IAM_TARGET_CLOUD_ID` in a dedicated
preconfigured environment. It checks the resolved cloud ID and secret absence.
It does not print or place the token in HCL. Verify the wrapper on the existing
deployment: delegation and EDS Create, temporary-secret DROP followed by a fresh
query, fresh-token Update and cleanup, and exact-pair Revoke/destroy. Include
least-privilege caller/runtime checks and provider state rollback. Runtime restart
or server rolling-deployment exercises are optional extra validation, not required
deployment steps or release gates; they require separate authorization.
