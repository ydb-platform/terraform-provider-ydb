package externaldatasource

import (
	"context"
	"errors"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIAMUnknownUpdateRetainsRecoveryState(t *testing.T) {
	d := schema.TestResourceDataRaw(t, map[string]*schema.Schema{
		"pending_cleanup_path": {Type: schema.TypeString, Computed: true},
		"pending_iam_mutation": {Type: schema.TypeString, Computed: true},
	}, nil)
	d.SetId("owned-eds")
	diags := completeIAMSecret(context.Background(), d, nil, "/owner/temporary", status.Error(codes.Unavailable, "response lost"))
	require.True(t, diags.HasError())
	require.Equal(t, "owned-eds", d.Id())
	require.Equal(t, "UNKNOWN", d.Get("pending_iam_mutation"))
	require.Equal(t, "/owner/temporary", d.Get("pending_cleanup_path"))
}

func TestIAMUnknownCreateDoesNotInventOwnership(t *testing.T) {
	d := schema.TestResourceDataRaw(t, map[string]*schema.Schema{
		"pending_cleanup_path": {Type: schema.TypeString, Computed: true},
		"pending_iam_mutation": {Type: schema.TypeString, Computed: true},
	}, nil)
	diags := completeIAMSecret(context.Background(), d, nil, "/owner/temporary", status.Error(codes.Unavailable, "response lost"))
	require.True(t, diags.HasError())
	require.Empty(t, d.Id())
	require.Empty(t, d.Get("pending_iam_mutation"))
	require.Contains(t, diags[0].Summary, "/owner/temporary")
}

func TestIAMOutcomeClassification(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		unknown bool
	}{
		{"acknowledged", nil, false},
		{"collision", status.Error(codes.AlreadyExists, ""), false},
		{"denied", status.Error(codes.PermissionDenied, ""), false},
		{"invalid", status.Error(codes.InvalidArgument, ""), false},
		{"lost response", status.Error(codes.Unavailable, ""), true},
		{"deadline", context.DeadlineExceeded, true},
		{"cancelled", context.Canceled, true},
		{"unclassified", errors.New("unknown error"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.unknown, iamOutcomeUnknown(tc.err))
		})
	}
}
