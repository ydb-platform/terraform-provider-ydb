package resourcepool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

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
	SetTotalMemoryLimit            bool
}

func resourcePoolFromData(d *schema.ResourceData) resourcePool {
	pool := resourcePool{
		Name:                           d.Get("name").(string),
		ConcurrentQueryLimit:           d.Get("concurrent_query_limit").(int),
		QueueSize:                      d.Get("queue_size").(int),
		DatabaseLoadCPUThreshold:       d.Get("database_load_cpu_threshold").(int),
		ResourceWeight:                 d.Get("resource_weight").(int),
		TotalCPULimitPercentPerNode:    d.Get("total_cpu_limit_percent_per_node").(float64),
		QueryCPULimitPercentPerNode:    d.Get("query_cpu_limit_percent_per_node").(float64),
		TotalMemoryLimitPercentPerNode: d.Get("total_memory_limit_percent_per_node").(float64),
	}
	pool.SetTotalMemoryLimit = pool.TotalMemoryLimitPercentPerNode != -1
	return pool
}

func resourcePoolSettings(pool resourcePool) []string {
	settings := []string{
		fmt.Sprintf("CONCURRENT_QUERY_LIMIT = %d", pool.ConcurrentQueryLimit),
		fmt.Sprintf("QUEUE_SIZE = %d", pool.QueueSize),
		fmt.Sprintf("DATABASE_LOAD_CPU_THRESHOLD = %d", pool.DatabaseLoadCPUThreshold),
		fmt.Sprintf("RESOURCE_WEIGHT = %d", pool.ResourceWeight),
		"TOTAL_CPU_LIMIT_PERCENT_PER_NODE = " + quoteString(formatFloat(pool.TotalCPULimitPercentPerNode)),
		"QUERY_CPU_LIMIT_PERCENT_PER_NODE = " + quoteString(formatFloat(pool.QueryCPULimitPercentPerNode)),
	}
	if pool.SetTotalMemoryLimit {
		settings = append(
			settings,
			"TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE = "+quoteString(formatFloat(pool.TotalMemoryLimitPercentPerNode)),
		)
	}
	return settings
}

func buildCreateQuery(pool resourcePool) string {
	return fmt.Sprintf(
		"CREATE RESOURCE POOL %s WITH (\n    %s\n)",
		quoteIdentifier(pool.Name),
		strings.Join(resourcePoolSettings(pool), ",\n    "),
	)
}

func buildAlterQuery(pool resourcePool) string {
	return fmt.Sprintf(
		"ALTER RESOURCE POOL %s SET (\n    %s\n)",
		quoteIdentifier(pool.Name),
		strings.Join(resourcePoolSettings(pool), ",\n    "),
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

	resultSet, err := db.Query().QueryResultSet(ctx, `
SELECT *
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
	if err != nil {
		return diag.Errorf("failed to read resource pool %q from .sys/resource_pools: %s", entity.GetEntityPath(), err)
	}
	defer func() { _ = resultSet.Close(ctx) }()

	row, err := resultSet.NextRow(ctx)
	if errors.Is(err, io.EOF) {
		d.SetId("")
		return nil
	}
	if err != nil {
		return diag.Errorf("failed to read resource pool %q row from .sys/resource_pools: %s", entity.GetEntityPath(), err)
	}

	err = row.ScanNamed(
		query.Named("Name", &name),
		query.Named("ConcurrentQueryLimit", &concurrentQueryLimit),
		query.Named("QueueSize", &queueSize),
		query.Named("DatabaseLoadCpuThreshold", &databaseLoadCPUThreshold),
		query.Named("ResourceWeight", &resourceWeight),
		query.Named("TotalCpuLimitPercentPerNode", &totalCPULimitPercentPerNode),
		query.Named("QueryCpuLimitPercentPerNode", &queryCPULimitPercentPerNode),
	)
	if err != nil {
		return diag.Errorf("failed to scan resource pool %q from .sys/resource_pools: %s", entity.GetEntityPath(), err)
	}
	totalMemoryLimitPercentPerNode = -1
	if hasColumn(resultSet.Columns(), "TotalMemoryLimitPercentPerNode") {
		if err = row.ScanNamed(
			query.Named("TotalMemoryLimitPercentPerNode", &totalMemoryLimitPercentPerNode),
		); err != nil {
			return diag.Errorf(
				"failed to scan resource pool %q memory limit from .sys/resource_pools: %s",
				entity.GetEntityPath(),
				err,
			)
		}
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
	if d.HasChange("total_memory_limit_percent_per_node") {
		oldValue, _ := d.GetChange("total_memory_limit_percent_per_node")
		pool.SetTotalMemoryLimit = pool.SetTotalMemoryLimit || oldValue.(float64) != -1
	}

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

func hasColumn(columns []string, name string) bool {
	for _, column := range columns {
		if column == name {
			return true
		}
	}
	return false
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
