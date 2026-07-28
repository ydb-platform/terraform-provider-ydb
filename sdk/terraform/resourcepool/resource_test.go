package resourcepool

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourcePoolSchemaValidation(t *testing.T) {
	schema := ResourceSchema()

	weightWarnings, weightErrors := schema["resource_weight"].ValidateFunc(100, "resource_weight")
	assert.Empty(t, weightWarnings)
	assert.Empty(t, weightErrors)

	_, weightErrors = schema["resource_weight"].ValidateFunc(101, "resource_weight")
	require.Len(t, weightErrors, 1)

	validatePercent := schema["total_cpu_limit_percent_per_node"].ValidateFunc
	for _, value := range []float64{-1, 0, 50.5, 100} {
		warnings, errors := validatePercent(value, "total_cpu_limit_percent_per_node")
		assert.Empty(t, warnings)
		assert.Empty(t, errors)
	}

	_, errors := validatePercent(-0.5, "total_cpu_limit_percent_per_node")
	require.Len(t, errors, 1)
}
