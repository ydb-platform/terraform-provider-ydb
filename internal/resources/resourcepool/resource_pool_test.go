package resourcepool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildResourcePoolQueries(t *testing.T) {
	pool := resourcePool{
		Name:                           "pool`name",
		ConcurrentQueryLimit:           4,
		QueueSize:                      16,
		DatabaseLoadCPUThreshold:       80,
		ResourceWeight:                 50,
		TotalCPULimitPercentPerNode:    70.5,
		QueryCPULimitPercentPerNode:    35,
		TotalMemoryLimitPercentPerNode: 65.25,
		SetTotalMemoryLimit:            true,
	}

	assert.Equal(t, `CREATE RESOURCE POOL `+"`pool``name`"+` WITH (
    CONCURRENT_QUERY_LIMIT = 4,
    QUEUE_SIZE = 16,
    DATABASE_LOAD_CPU_THRESHOLD = 80,
    RESOURCE_WEIGHT = 50,
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE = '70.5',
    QUERY_CPU_LIMIT_PERCENT_PER_NODE = '35',
    TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE = '65.25'
)`, buildCreateQuery(pool))

	assert.Equal(t, `ALTER RESOURCE POOL `+"`pool``name`"+` SET (
    CONCURRENT_QUERY_LIMIT = 4,
    QUEUE_SIZE = 16,
    DATABASE_LOAD_CPU_THRESHOLD = 80,
    RESOURCE_WEIGHT = 50,
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE = '70.5',
    QUERY_CPU_LIMIT_PERCENT_PER_NODE = '35',
    TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE = '65.25'
)`, buildAlterQuery(pool))
}

func TestBuildResourcePoolQueriesWithoutTotalMemoryLimit(t *testing.T) {
	pool := resourcePool{
		Name:                           "pool",
		ConcurrentQueryLimit:           4,
		QueueSize:                      16,
		DatabaseLoadCPUThreshold:       80,
		ResourceWeight:                 50,
		TotalCPULimitPercentPerNode:    70.5,
		QueryCPULimitPercentPerNode:    35,
		TotalMemoryLimitPercentPerNode: -1,
	}

	assert.NotContains(t, buildCreateQuery(pool), "TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE")
	assert.NotContains(t, buildAlterQuery(pool), "TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE")
}

func TestBuildResourcePoolQueriesWithNegativeSettings(t *testing.T) {
	pool := resourcePool{
		Name:                           "pool",
		ConcurrentQueryLimit:           -1,
		QueueSize:                      -1,
		DatabaseLoadCPUThreshold:       -1,
		ResourceWeight:                 -1,
		TotalCPULimitPercentPerNode:    -1,
		QueryCPULimitPercentPerNode:    -1,
		TotalMemoryLimitPercentPerNode: -1,
	}

	assert.Equal(t, `CREATE RESOURCE POOL `+"`pool`"+` WITH (
    CONCURRENT_QUERY_LIMIT = '-1',
    QUEUE_SIZE = '-1',
    DATABASE_LOAD_CPU_THRESHOLD = '-1',
    RESOURCE_WEIGHT = '-1',
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE = '-1',
    QUERY_CPU_LIMIT_PERCENT_PER_NODE = '-1'
)`, buildCreateQuery(pool))

	assert.Equal(t, `ALTER RESOURCE POOL `+"`pool`"+` SET (
    CONCURRENT_QUERY_LIMIT = '-1',
    QUEUE_SIZE = '-1',
    DATABASE_LOAD_CPU_THRESHOLD = '-1',
    RESOURCE_WEIGHT = '-1',
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE = '-1',
    QUERY_CPU_LIMIT_PERCENT_PER_NODE = '-1'
)`, buildAlterQuery(pool))
}
