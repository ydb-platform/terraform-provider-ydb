# ydb_resource_pool resource

`ydb_resource_pool` manages a YDB resource pool: a schema object that defines
limits on the CPU, memory, and query concurrency available to a group of
queries.

See the YDB documentation for
[`CREATE RESOURCE POOL`](https://ydb.tech/docs/ru/yql/reference/syntax/create-resource-pool?version=main),
[`ALTER RESOURCE POOL`](https://ydb.tech/docs/ru/yql/reference/syntax/alter-resource-pool?version=main),
and
[`DROP RESOURCE POOL`](https://ydb.tech/docs/ru/yql/reference/syntax/drop-resource-pool?version=main).

## Example

```tf
resource "ydb_resource_pool" "olap" {
  connection_string                   = "grpc://localhost:2136/?database=/local"
  name                                = "olap"
  concurrent_query_limit              = 20
  queue_size                          = 1000
  database_load_cpu_threshold         = 80
  resource_weight                     = 100
  total_cpu_limit_percent_per_node    = 70
  query_cpu_limit_percent_per_node    = 50
  total_memory_limit_percent_per_node = 80
}
```

## Argument Reference

### Required

- `connection_string` (String) — YDB database connection string.
- `name` (String) — unique resource pool name. Path notation is not allowed, so
  the name must not contain `/`. The built-in `default` pool cannot be created
  or deleted.

### Optional

- `concurrent_query_limit` (Int32, default `-1`) — number of queries that may
  execute concurrently. Allowed values are `-1` or `0` through `2^31 - 1`.
  `-1` disables the limit. With `0`, queries sent to the pool immediately fail
  with `PRECONDITION_FAILED`.
- `queue_size` (Int32, default `-1`) — maximum number of queries in the waiting
  queue. Allowed values are `-1` or `0` through `2^31 - 1`; `-1` disables the
  limit. At most `concurrent_query_limit + queue_size` queries may be in the
  system for this pool at the same time.
- `database_load_cpu_threshold` (Int32, default `-1`) — database CPU load
  threshold after which queries remain queued instead of starting. Allowed
  values are `-1` or `0` through `100`; `-1` disables the threshold.
- `total_memory_limit_percent_per_node` (Double, default `-1`) — percentage of
  memory on one node available to all queries in the pool. Allowed values are
  `-1` or `0` through `100`; `-1` disables the limit. When the pool reaches the
  limit, new queries that require memory and running queries that request more
  memory fail with `OVERLOADED`.
- `total_cpu_limit_percent_per_node` (Double, default `-1`) — percentage of CPU
  on one node available to all queries in the pool. Allowed values are `-1` or
  `0` through `100`; `-1` disables the limit.
- `query_cpu_limit_percent_per_node` (Double, default `-1`) — percentage of CPU
  on one node available to a single query in the pool. Allowed values are `-1`
  or `0` through `100`; `-1` disables the limit.
- `resource_weight` (Int32, default `-1`) — weight used to distribute resources
  between pools. Current YDB `edge` accepts `-1` or `0` through `100`; `-1`
  disables weighted distribution.

The YDB main documentation currently lists `2^31 - 1` as the upper bound for
`RESOURCE_WEIGHT`, but the current `edge` server validates it as a percentage
and rejects values greater than `100`. The provider follows the server behavior
so invalid plans fail during Terraform validation.

YDB main does not currently support `QUERY_MEMORY_LIMIT_PERCENT_PER_NODE`.
Use `total_memory_limit_percent_per_node` to limit pool memory instead.

The current `ydbplatform/local-ydb:edge` image predates
`TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE`. When the argument is omitted, the
provider remains compatible with that image and reports the value as `-1`.
Configuring the argument requires a newer YDB build that supports the property.

## Permissions

YDB requires:

- `CREATE TABLE` on `.metadata/workload_manager/pools` to create a pool;
- `ALTER SCHEMA` on the pool in `.metadata/workload_manager/pools` to update it;
- `REMOVE SCHEMA` on `.metadata/workload_manager/pools` to delete it.

The YDB cluster must have the `EnableResourcePools` feature flag enabled.
