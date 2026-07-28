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
