package table

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
			expected: "P174W2DT12H", // BUT ONLY WEEK MAX UNIT (You can't use units of measurement exceeding one week.)
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
