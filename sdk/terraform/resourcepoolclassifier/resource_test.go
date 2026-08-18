package resourcepoolclassifier

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourcePoolClassifierSchemaValidation(t *testing.T) {
	resourceSchema := ResourceSchema()

	validateRank := resourceSchema["rank"].ValidateFunc
	for _, value := range []int{0, 1000} {
		warnings, errors := validateRank(value, "rank")
		assert.Empty(t, warnings)
		assert.Empty(t, errors)
	}

	_, rankErrors := validateRank(-1, "rank")
	require.Len(t, rankErrors, 1)

	_, poolNameErrors := resourceSchema["resource_pool"].ValidateFunc("folder/pool", "resource_pool")
	require.Len(t, poolNameErrors, 1)
}
