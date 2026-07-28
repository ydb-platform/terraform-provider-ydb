package resourcepool

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

type resourcePool struct {
	Name                           string
	ConcurrentQueryLimit           int
	QueueSize                      int
	DatabaseLoadCPUThreshold       int
	ResourceWeight                 int
	TotalCPULimitPercentPerNode    float64
	QueryCPULimitPercentPerNode    float64
	TotalMemoryLimitPercentPerNode float64
}

func resourcePoolFromData(d *schema.ResourceData) resourcePool {
	return resourcePool{
		Name:                           d.Get("name").(string),
		ConcurrentQueryLimit:           d.Get("concurrent_query_limit").(int),
		QueueSize:                      d.Get("queue_size").(int),
		DatabaseLoadCPUThreshold:       d.Get("database_load_cpu_threshold").(int),
		ResourceWeight:                 d.Get("resource_weight").(int),
		TotalCPULimitPercentPerNode:    d.Get("total_cpu_limit_percent_per_node").(float64),
		QueryCPULimitPercentPerNode:    d.Get("query_cpu_limit_percent_per_node").(float64),
		TotalMemoryLimitPercentPerNode: d.Get("total_memory_limit_percent_per_node").(float64),
	}
}

func buildCreateQuery(pool resourcePool) string {
	return fmt.Sprintf(`CREATE RESOURCE POOL %s WITH (
    CONCURRENT_QUERY_LIMIT = %d,
    QUEUE_SIZE = %d,
    DATABASE_LOAD_CPU_THRESHOLD = %d,
    RESOURCE_WEIGHT = %d,
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE = %s,
    QUERY_CPU_LIMIT_PERCENT_PER_NODE = %s,
    TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE = %s
)`,
		quoteIdentifier(pool.Name),
		pool.ConcurrentQueryLimit,
		pool.QueueSize,
		pool.DatabaseLoadCPUThreshold,
		pool.ResourceWeight,
		quoteString(formatFloat(pool.TotalCPULimitPercentPerNode)),
		quoteString(formatFloat(pool.QueryCPULimitPercentPerNode)),
		quoteString(formatFloat(pool.TotalMemoryLimitPercentPerNode)),
	)
}

func buildAlterQuery(pool resourcePool) string {
	return fmt.Sprintf(`ALTER RESOURCE POOL %s SET (
    CONCURRENT_QUERY_LIMIT = %d,
    QUEUE_SIZE = %d,
    DATABASE_LOAD_CPU_THRESHOLD = %d,
    RESOURCE_WEIGHT = %d,
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE = %s,
    QUERY_CPU_LIMIT_PERCENT_PER_NODE = %s,
    TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE = %s
)`,
		quoteIdentifier(pool.Name),
		pool.ConcurrentQueryLimit,
		pool.QueueSize,
		pool.DatabaseLoadCPUThreshold,
		pool.ResourceWeight,
		quoteString(formatFloat(pool.TotalCPULimitPercentPerNode)),
		quoteString(formatFloat(pool.QueryCPULimitPercentPerNode)),
		quoteString(formatFloat(pool.TotalMemoryLimitPercentPerNode)),
	)
}

func quoteIdentifier(name string) string {
	return "`" + helpers.EscapeYQLIdentifier(name) + "`"
}

func quoteString(value string) string {
	return "'" + helpers.EscapeYQLString(value) + "'"
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func (h *handler) Create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	connectionString := d.Get("connection_string").(string)
	pool := resourcePoolFromData(d)

	db, err := h.openDB(ctx, connectionString)
	if err != nil {
		return diag.FromErr(err)
	}
	defer func() { _ = db.Close(ctx) }()

	if err = db.Query().Exec(ctx, buildCreateQuery(pool)); err != nil {
		return diag.Errorf("failed to execute CREATE RESOURCE POOL for %q: %s", pool.Name, err)
	}

	d.SetId(connectionString + "?path=" + pool.Name)
	return h.Read(ctx, d, meta)
}

func (h *handler) Read(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	db, err := h.openDB(ctx, entity.PrepareFullYDBEndpoint())
	if err != nil {
		return diag.FromErr(err)
	}
	defer func() { _ = db.Close(ctx) }()

	var (
		name                           string
		concurrentQueryLimit           int32
		queueSize                      int32
		databaseLoadCPUThreshold       float64
		resourceWeight                 float64
		totalCPULimitPercentPerNode    float64
		queryCPULimitPercentPerNode    float64
		totalMemoryLimitPercentPerNode float64
	)

	row, err := db.Query().QueryRow(ctx, `
SELECT
    Name,
    ConcurrentQueryLimit,
    QueueSize,
    DatabaseLoadCpuThreshold,
    ResourceWeight,
    TotalCpuLimitPercentPerNode,
    QueryCpuLimitPercentPerNode,
    TotalMemoryLimitPercentPerNode
FROM `+"`"+`.sys/resource_pools`+"`"+`
WHERE Name = $name;
`,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$name").Text(entity.GetEntityPath()).
				Build(),
		),
		query.WithIdempotent(),
	)
	if errors.Is(err, query.ErrNoRows) {
		d.SetId("")
		return nil
	}
	if err != nil {
		return diag.Errorf("failed to read resource pool %q from .sys/resource_pools: %s", entity.GetEntityPath(), err)
	}

	err = row.ScanNamed(
		query.Named("Name", &name),
		query.Named("ConcurrentQueryLimit", &concurrentQueryLimit),
		query.Named("QueueSize", &queueSize),
		query.Named("DatabaseLoadCpuThreshold", &databaseLoadCPUThreshold),
		query.Named("ResourceWeight", &resourceWeight),
		query.Named("TotalCpuLimitPercentPerNode", &totalCPULimitPercentPerNode),
		query.Named("QueryCpuLimitPercentPerNode", &queryCPULimitPercentPerNode),
		query.Named("TotalMemoryLimitPercentPerNode", &totalMemoryLimitPercentPerNode),
	)
	if err != nil {
		return diag.Errorf("failed to scan resource pool %q from .sys/resource_pools: %s", entity.GetEntityPath(), err)
	}

	if err = setResourcePoolState(d, entity.PrepareFullYDBEndpoint(), resourcePool{
		Name:                           name,
		ConcurrentQueryLimit:           int(concurrentQueryLimit),
		QueueSize:                      int(queueSize),
		DatabaseLoadCPUThreshold:       int(databaseLoadCPUThreshold),
		ResourceWeight:                 int(resourceWeight),
		TotalCPULimitPercentPerNode:    totalCPULimitPercentPerNode,
		QueryCPULimitPercentPerNode:    queryCPULimitPercentPerNode,
		TotalMemoryLimitPercentPerNode: totalMemoryLimitPercentPerNode,
	}); err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func (h *handler) Update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	pool := resourcePoolFromData(d)
	pool.Name = entity.GetEntityPath()

	db, err := h.openDB(ctx, entity.PrepareFullYDBEndpoint())
	if err != nil {
		return diag.FromErr(err)
	}
	defer func() { _ = db.Close(ctx) }()

	if err = db.Query().Exec(ctx, buildAlterQuery(pool)); err != nil {
		return diag.Errorf("failed to execute ALTER RESOURCE POOL for %q: %s", pool.Name, err)
	}

	return h.Read(ctx, d, meta)
}

func (h *handler) Delete(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	db, err := h.openDB(ctx, entity.PrepareFullYDBEndpoint())
	if err != nil {
		return diag.FromErr(err)
	}
	defer func() { _ = db.Close(ctx) }()

	err = db.Query().Exec(ctx, "DROP RESOURCE POOL "+quoteIdentifier(entity.GetEntityPath()))
	if err != nil && !ydb.IsOperationErrorSchemeError(err) && !ydb.IsOperationErrorNotFoundError(err) {
		return diag.Errorf("failed to execute DROP RESOURCE POOL for %q: %s", entity.GetEntityPath(), err)
	}

	d.SetId("")
	return nil
}

func (h *handler) openDB(ctx context.Context, connectionString string) (*ydb.Driver, error) {
	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: connectionString,
		AuthCreds:        h.authCreds,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize YDB client: %w", err)
	}
	return db, nil
}

func setResourcePoolState(d *schema.ResourceData, connectionString string, pool resourcePool) error {
	values := map[string]interface{}{
		"connection_string":                   connectionString,
		"name":                                pool.Name,
		"concurrent_query_limit":              pool.ConcurrentQueryLimit,
		"queue_size":                          pool.QueueSize,
		"database_load_cpu_threshold":         pool.DatabaseLoadCPUThreshold,
		"resource_weight":                     pool.ResourceWeight,
		"total_cpu_limit_percent_per_node":    pool.TotalCPULimitPercentPerNode,
		"query_cpu_limit_percent_per_node":    pool.QueryCPULimitPercentPerNode,
		"total_memory_limit_percent_per_node": pool.TotalMemoryLimitPercentPerNode,
	}
	for key, value := range values {
		if err := d.Set(key, value); err != nil {
			return fmt.Errorf("failed to set resource pool attribute %q: %w", key, err)
		}
	}
	return nil
}
