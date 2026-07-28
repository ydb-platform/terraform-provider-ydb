package resourcepoolclassifier

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildCreateResourcePoolClassifierQuery(t *testing.T) {
	classifier := resourcePoolClassifier{
		Name:         "classifier`name",
		Rank:         42,
		ResourcePool: "pool'name",
		MemberName:   "member'name",
		HasRank:      true,
		HasMember:    true,
	}

	assert.Equal(t, `CREATE RESOURCE POOL CLASSIFIER `+"`classifier``name`"+` WITH (
    RESOURCE_POOL = 'pool\'name',
    RANK = 42,
    MEMBER_NAME = 'member\'name'
)`, buildCreateQuery(classifier))
}

func TestBuildAlterResourcePoolClassifierQueryResetsMemberName(t *testing.T) {
	classifier := resourcePoolClassifier{
		Name:         "classifier",
		Rank:         43,
		ResourcePool: "pool",
	}

	assert.Equal(t, `ALTER RESOURCE POOL CLASSIFIER `+"`classifier`"+` SET (
    RANK = 43,
    RESOURCE_POOL = 'pool'
), RESET (MEMBER_NAME)`, buildAlterQuery(classifier))
}
