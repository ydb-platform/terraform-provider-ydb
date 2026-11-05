package externaldatasource

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
