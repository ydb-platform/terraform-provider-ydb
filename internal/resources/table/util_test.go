package table

import (
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpandColumns(t *testing.T) {
	set := schema.NewSet(func(v interface{}) int {
		m := v.(map[string]interface{})
		return schema.HashString(m["name"].(string))
	}, []interface{}{
		map[string]interface{}{"name": "a", "type": "Uint32", "not_null": false},
	})
	cols := expandColumns(set)
	require.Len(t, cols, 1)
	assert.Equal(t, "a", cols[0].Name)
	assert.Equal(t, "Uint32", cols[0].Type)
}

func TestTTLToISO8601(t *testing.T) {
	const (
		day   = 24 * time.Hour
		month = 30 * day
		year  = 365 * day
	)

	testData := []struct {
		testName string
		ttl      time.Duration
		expected string
	}{
		{
			testName: "zero ttl",
			ttl:      0,
			expected: "",
		},
		{
			testName: "two days",
			ttl:      48 * time.Hour,
			expected: "P2D",
		},
		{
			testName: "one hour",
			ttl:      time.Hour,
			expected: "PT1H",
		},
		{
			testName: "three years four month five days twelve hours, BUT ONLY WEEK MAX UNIT",
			ttl:      3*year + 4*month + 5*day + 12*time.Hour, // 1220 days + 12 hours
			expected: "P174W2DT12H",                           // BUT ONLY WEEK MAX UNIT (You can't use units of measurement exceeding one week.) 1220 days + 12 hours
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := ttlToISO8601(v.ttl)
			assert.Equal(t, v.expected, got)
		})
	}
}
