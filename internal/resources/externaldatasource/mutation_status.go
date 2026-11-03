package externaldatasource

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mutationOutcome uint8

const (
	notSubmitted mutationOutcome = iota
	terminalSuccess
	terminalFailure
	unknownOutcome
)

type mutationResult struct {
	outcome mutationOutcome
	err     error
}

// safeStatus deliberately excludes server issue strings, RPC messages and query
// parameter dumps: those may echo a credential, even though SQL is parameterized.
func safeStatus(err error) string {
	if err == nil {
		return "OK"
	}
	if e := ydb.OperationError(err); e != nil {
		return fmt.Sprintf("YDB status %d", e.Code())
	}
	if e := ydb.TransportError(err); e != nil {
		return fmt.Sprintf("transport status %d", e.Code())
	}
	if s, ok := status.FromError(err); ok {
		return "RPC " + s.Code().String()
	}
	if errors.Is(err, context.Canceled) {
		return "context cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline exceeded"
	}
	return "request failed (details omitted to protect credentials)"
}

func classifyMutation(err error) mutationResult {
	if err == nil {
		return mutationResult{outcome: terminalSuccess}
	}
	// A terminal rejection is different from a lost/cancelled response. In
	// particular, TIMEOUT/CANCELLED/UNAVAILABLE/UNDETERMINED cannot prove rollback.
	if ydb.IsOperationError(err, Ydb.StatusIds_BAD_REQUEST, Ydb.StatusIds_UNAUTHORIZED, Ydb.StatusIds_PRECONDITION_FAILED, Ydb.StatusIds_ALREADY_EXISTS, Ydb.StatusIds_NOT_FOUND, Ydb.StatusIds_SCHEME_ERROR, Ydb.StatusIds_UNSUPPORTED) {
		return mutationResult{outcome: terminalFailure, err: err}
	}
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated, codes.InvalidArgument, codes.AlreadyExists, codes.NotFound, codes.FailedPrecondition, codes.Unimplemented:
		return mutationResult{outcome: terminalFailure, err: err}
	}
	return mutationResult{outcome: unknownOutcome, err: err}
}

func execMutation(ctx context.Context, b backend, q string, opts ...query.ExecuteOption) mutationResult {
	if err := ctx.Err(); err != nil {
		return mutationResult{outcome: notSubmitted, err: err}
	}
	return classifyMutation(b.Exec(ctx, q, opts...))
}

func (h *Handler) boundedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := h.closeTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func (h *Handler) close(ctx context.Context, b backend) {
	closeCtx, cancel := h.boundedContext(ctx)
	defer cancel()
	_ = b.Close(closeCtx)
}

// An uncertain EDS update may still be running. Reading an equal description
// does not establish termination; retain identity and block further operations.
func guardIAMMutation(d *schema.ResourceData) diag.Diagnostics {
	uncertain, _ := d.Get("pending_iam_mutation").(string)
	if uncertain != "" {
		return diag.Errorf("IAM mutation outcome remains unknown. Verify the server operation has stopped before manual recovery. See IAM recovery documentation")
	}
	return nil
}
