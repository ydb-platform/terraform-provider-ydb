package externaldatasource

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"
)

func TestIAMCancelledBeforeSubmissionRetainsKnownOutcome(t *testing.T) {
	d := schema.TestResourceDataRaw(t, map[string]*schema.Schema{
		"pending_cleanup_path": {Type: schema.TypeString, Computed: true},
		"pending_iam_mutation": {Type: schema.TypeString, Computed: true},
	}, nil)
	d.SetId("owned-eds")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	diags := cancelIAMSubmission(ctx, d, nil, "", time.Minute)

	require.True(t, diags.HasError())
	require.Equal(t, "owned-eds", d.Id())
	require.Empty(t, d.Get("pending_iam_mutation"), "a request that was not submitted cannot have an unknown server outcome")
	require.Empty(t, d.Get("pending_cleanup_path"))
	require.Contains(t, diags[0].Summary, "not submitted")
}
