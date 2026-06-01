# YDB Terraform Provider

This provider can be used for managing YDB schema resources in managed or on-premise installations of YDB databases.

**NOTE**: API is experimental and can be changed or broken.

## Available resources
- [ydb_table](./internal/resources/table/README.md)
- [ydb_table_index](./internal/resources/table/index/README.md)
- [ydb_table_changefeed](./internal/resources/changefeed/README.md)
- [ydb_external_data_source](./internal/resources/externaldatasource/README.md)
- [ydb_external_table](./internal/resources/externaltable/README.md)
- [ydb_secret](./internal/resources/secret/README.md)

## Acceptance tests

Acceptance tests live in `internal/terraform/` and run real Terraform plans against a YDB instance. They require:

- `terraform` CLI on `PATH`
- a running YDB endpoint (local Docker is fine — see below)
- `TF_ACC=1` (required by the Terraform SDK to enable acceptance tests)
- `YDB_ACC_CONNECTION_STRING` — full YDB connection string (e.g. `grpc://127.0.0.1:2136/?database=/local`)

Optional provider auth env vars: `YDB_ACC_TOKEN`, `YDB_ACC_USER`, `YDB_ACC_PASSWORD`.

### Spin up a local YDB

```sh
docker run -d --rm --name ydb-local \
  -p 2135:2135 -p 2136:2136 -p 8765:8765 \
  -h localhost \
  -e YDB_USE_IN_MEMORY_PDISKS=true \
  -e YDB_FEATURE_FLAGS=enable_replace_if_exists_for_external_entities,enable_external_data_sources,enable_schema_secrets \
  ydbplatform/local-ydb:25.4
```

### Run all acceptance tests

```sh
YDB_ACC_CONNECTION_STRING=grpc://127.0.0.1:2136/?database=/local TF_ACC=1 \
  go test -v ./internal/terraform/... -timeout 30m
```

### Run a single test

```sh
YDB_ACC_CONNECTION_STRING=grpc://127.0.0.1:2136/?database=/local TF_ACC=1 \
  go test -v ./internal/terraform/ -run TestAccYdbTopic_metricsLevel -timeout 30m
```