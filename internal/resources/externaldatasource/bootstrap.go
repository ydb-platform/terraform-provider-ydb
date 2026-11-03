package externaldatasource

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

type bootstrapStage uint8

const (
	secretStage bootstrapStage = iota
	edsStage
)

type bootstrapResult struct {
	mutationResult
	stage      bootstrapStage
	path       string
	owned      bool
	cleanupErr error
}

var errCallerTokenRequired = errors.New("caller IAM token required")

func isCollision(err error) bool {
	return ydb.IsOperationErrorAlreadyExistsError(err) || status.Code(err) == codes.AlreadyExists
}

func (h *Handler) freshBackend(ctx context.Context, endpoint string) (backend, error) {
	creds := h.authCreds
	if h.callerAuth != nil {
		var err error
		creds, err = h.callerAuth(ctx)
		if err != nil {
			return nil, err
		}
	}
	if h.openBackend != nil {
		return h.openBackend(ctx, endpoint, creds)
	}
	return openDriverBackend(ctx, endpoint, creds)
}

func (h *Handler) cleanup(ctx context.Context, endpoint, p string) error {
	cleanCtx, cancel := h.boundedContext(ctx)
	defer cancel()
	b, err := h.freshBackend(cleanCtx, endpoint)
	if err != nil {
		return err
	}
	defer h.close(cleanCtx, b)
	err = b.Exec(cleanCtx, "DROP SECRET `"+helpers.EscapeYQLIdentifier(p)+"`")
	if err == nil {
		return nil
	}
	// Lost DROP response and NotFound are safe only after exact absence is
	// confirmed by a successful parent listing. Never infer it from SCHEME_ERROR.
	absent, probeErr := b.PathAbsent(cleanCtx, p)
	if probeErr == nil && absent {
		return nil
	}
	return err
}

func (h *Handler) bootstrap(ctx context.Context, endpoint, root string, b backend, mutation func(string) mutationResult) bootstrapResult {
	if err := ctx.Err(); err != nil {
		return bootstrapResult{mutationResult: mutationResult{outcome: notSubmitted, err: err}}
	}
	creds := h.authCreds
	if h.callerAuth != nil {
		var err error
		creds, err = h.callerAuth(ctx)
		if err != nil {
			return bootstrapResult{mutationResult: mutationResult{outcome: notSubmitted, err: err}}
		}
	}
	if creds.Token == "" {
		return bootstrapResult{mutationResult: mutationResult{outcome: notSubmitted, err: errCallerTokenRequired}}
	}
	var p string
	for attempt := 0; attempt < 5; attempt++ {
		name := "__tf_iam_bootstrap_" + uuid.NewString()
		if h.uniqueName != nil {
			name = h.uniqueName()
		}
		p = path.Join(root, name)
		q := "DECLARE $value AS Utf8; CREATE SECRET `" + helpers.EscapeYQLIdentifier(p) + "` WITH (value = $value)"
		result := execMutation(ctx, b, q, query.WithParameters(ydb.ParamsBuilder().Param("$value").Text(creds.Token).Build()))
		if result.outcome == terminalSuccess {
			break
		}
		if result.outcome == terminalFailure && isCollision(result.err) {
			if attempt < 4 {
				continue
			}
			p = ""
		}
		return bootstrapResult{mutationResult: result, path: p}
	}
	result := bootstrapResult{mutationResult: mutation(p), stage: edsStage, path: p, owned: true}
	if result.outcome == unknownOutcome {
		return result
	}
	result.cleanupErr = h.cleanup(ctx, endpoint, p)
	return result
}

func (h *Handler) recoverCleanup(ctx context.Context, d *schema.ResourceData, endpoint string) diag.Diagnostics {
	p, _ := d.Get("pending_cleanup_path").(string)
	uncertain, _ := d.Get("pending_iam_mutation").(string)
	if uncertain != "" {
		if p != "" {
			return diag.Errorf("IAM mutation outcome remains unknown; confirmed-owned bootstrap secret %q is preserved. Verify the server operation has stopped before manual recovery; do not drop a possibly in-use secret. See IAM bootstrap recovery documentation", p)
		}
		return guardIAMMutation(d)
	}
	if p == "" {
		return nil
	}
	if err := h.cleanup(ctx, endpoint, p); err != nil {
		return diag.Errorf("cannot clean confirmed-owned bootstrap secret %q: %s; EDS identity and pending cleanup are retained", p, safeStatus(err))
	}
	return diag.FromErr(d.Set("pending_cleanup_path", ""))
}

func bootstrapDiagnostics(result bootstrapResult) diag.Diagnostics {
	if result.outcome == terminalSuccess {
		return nil
	}
	detail := "EDS mutation failed: " + safeStatus(result.err)
	if result.outcome == unknownOutcome {
		if result.stage == edsStage {
			detail = "EDS mutation outcome is unknown: " + safeStatus(result.err) + ". Reconcile server-side execution; an equal Describe does not establish Create ownership. Import only after verifying ownership"
		} else {
			detail = "Secret CREATE outcome is unknown: " + safeStatus(result.err) + ". EDS mutation was not submitted"
		}
	}
	if result.path != "" && !result.owned {
		detail += ". Secret CREATE was not acknowledged; ownership of path " + fmt.Sprintf("%q", result.path) + " is unconfirmed. Do not delete it without independent ownership evidence"
	}
	if result.owned && result.outcome == unknownOutcome {
		detail += ". Confirmed-owned bootstrap secret " + fmt.Sprintf("%q", result.path) + " is preserved while the mutation may still be running"
	}
	if result.owned && result.cleanupErr != nil {
		detail += ". Cleanup also failed (" + safeStatus(result.cleanupErr) + "); confirmed-owned orphan " + fmt.Sprintf("%q", result.path) + " requires manual DROP SECRET after verifying terminal mutation. No EDS ownership was synthesized"
	}
	return diag.Errorf("%s", detail)
}
