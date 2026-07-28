# ydb_resource_pool_classifier resource

`ydb_resource_pool_classifier` manages a YDB resource pool classifier: a schema
object that routes matching queries to a resource pool.

See the YDB documentation for
[`CREATE RESOURCE POOL CLASSIFIER`](https://ydb.tech/docs/ru/yql/reference/syntax/create-resource-pool-classifier?version=main),
[`ALTER RESOURCE POOL CLASSIFIER`](https://ydb.tech/docs/ru/yql/reference/syntax/alter-resource-pool-classifier?version=main),
and
[`DROP RESOURCE POOL CLASSIFIER`](https://ydb.tech/docs/ru/yql/reference/syntax/drop-resource-pool-classifier?version=main).

## Example

```tf
resource "ydb_resource_pool_classifier" "olap" {
  connection_string = "grpc://localhost:2136/?database=/local"
  name              = "olap_classifier"
  rank              = 1000
  resource_pool     = ydb_resource_pool.olap.name
  member_name       = "all-users@well-known"
}
```

## Argument Reference

### Required

- `connection_string` (String) — YDB database connection string.
- `name` (String) — unique resource pool classifier name. It must not contain
  characters forbidden for YDB schema objects.
- `resource_pool` (String) — name of the resource pool to which matching queries
  are routed. A classifier may refer to a missing or inaccessible pool; YDB
  skips such a classifier when routing a query.

### Optional

- `rank` (Int64) — unique classifier evaluation order in the range `0` through
  `2^63 - 1`. If omitted, YDB assigns `MAX(existing ranks) + 1000`. Rank values
  must be unique so that classifier selection remains deterministic.
- `member_name` (String) — user or group SID to match. YDB compares this value
  character by character with the authenticated user's SID and every group SID
  in the authentication token. If omitted, the classifier ignores this
  criterion.

Common `member_name` formats are:

- `user1` for a built-in YDB user;
- `<subject_id>@as` for Access Service;
- `<login>@<domain>` for LDAP or an external identity provider;
- `all-users@well-known` to match every authenticated user.

## Permissions

YDB requires the `ALL` permission on the database to create, update, or delete a
resource pool classifier.

The YDB cluster must have the `EnableResourcePools` feature flag enabled.
