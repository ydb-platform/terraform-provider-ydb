package changefeed

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareCreateRequest_initialScan(t *testing.T) {
	format := "JSON"
	t.Run("omits when false", func(t *testing.T) {
		q := PrepareCreateRequest(&changeDataCaptureSettings{
			TablePath:   "my/table",
			Name:        "cf1",
			Mode:        "UPDATES",
			Format:      &format,
			InitialScan: false,
		})
		require.NotContains(t, q, "INITIAL_SCAN")
	})
	t.Run("includes when true", func(t *testing.T) {
		q := PrepareCreateRequest(&changeDataCaptureSettings{
			TablePath:   "my/table",
			Name:        "cf1",
			Mode:        "UPDATES",
			Format:      &format,
			InitialScan: true,
		})
		require.Contains(t, q, "INITIAL_SCAN = TRUE")
	})
}
