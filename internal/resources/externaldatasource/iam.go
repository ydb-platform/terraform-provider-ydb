package externaldatasource

import (
	"context"
	"crypto/rand"
	"fmt"
	"path"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

func (h *Handler) createIAMSecret(ctx context.Context, db *ydb.Driver, root string) (string, error) {
	if h.authCreds.Token == "" {
		return "", fmt.Errorf("IAM authentication requires the caller's IAM token")
	}
	p := path.Join(root, "__tf_iam_"+rand.Text())
	q := "DECLARE $value AS Utf8; CREATE SECRET `" + helpers.EscapeYQLIdentifier(p) + "` WITH (value = $value)"
	params := ydb.ParamsBuilder().Param("$value").Text(h.authCreds.Token).Build()
	if err := db.Query().Exec(ctx, q, query.WithParameters(params)); err != nil {
		return "", fmt.Errorf("temporary IAM secret CREATE was not acknowledged at %q (%s); EDS was not submitted. The server must support parameterized CREATE SECRET. Verify ownership and request termination before manual cleanup", p, iamStatus(err))
	}
	return p, nil
}

func dropIAMSecret(ctx context.Context, db *ydb.Driver, p string) error {
	err := db.Query().Exec(ctx, "DROP SECRET `"+helpers.EscapeYQLIdentifier(p)+"`")
	if err != nil {
		if absent, checkErr := iamPathAbsent(ctx, db, p); checkErr == nil && absent {
			return nil
		}
	}
	return err
}

// A SchemeError alone also covers an inaccessible or differently typed object.
func iamPathAbsent(ctx context.Context, db *ydb.Driver, p string) (bool, error) {
	dir, err := db.Scheme().ListDirectory(ctx, path.Dir(p))
	if err != nil {
		return false, err
	}
	for _, child := range dir.Children {
		if child.Name == path.Base(p) {
			return false, nil
		}
	}
	return true, nil
}

func completeIAMSecret(ctx context.Context, db *ydb.Driver, p string, err error) diag.Diagnostics {
	if iamOutcomeUnknown(err) {
		return diag.Errorf("IAM EDS outcome is unknown (%s); temporary secret %q is retained. Verify the server request has terminated before manual cleanup or another mutation", iamStatus(err), p)
	}
	var cleanupErr error
	if p != "" {
		cleanupErr = dropIAMSecret(ctx, db, p)
	}
	if cleanupErr != nil {
		if err == nil {
			return diag.Diagnostics{{Severity: diag.Warning, Summary: "IAM EDS saved; temporary secret cleanup pending", Detail: fmt.Sprintf("Secret %q: %s. Remove the owned secret manually.", p, iamStatus(cleanupErr))}}
		}
		return diag.Errorf("IAM EDS request failed (%s); temporary secret %q requires cleanup (%s)", iamStatus(err), p, iamStatus(cleanupErr))
	}
	if err != nil {
		return diag.Errorf("IAM EDS request failed: %s", iamStatus(err))
	}
	return nil
}

func cancelIAMSubmission(ctx context.Context, db *ydb.Driver, p string, timeout time.Duration) diag.Diagnostics {
	if p != "" {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		if err := dropIAMSecret(cleanupCtx, db, p); err != nil {
			return diag.Errorf("IAM EDS was not submitted (%s); temporary secret %q requires cleanup (%s)", ctx.Err(), p, iamStatus(err))
		}
	}
	return diag.Errorf("IAM EDS was not submitted: %s", ctx.Err())
}

func readIAMAfterWrite(ctx context.Context, d *schema.ResourceData, db *ydb.Driver) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}
	desc, err := db.Table().DescribeExternalDataSource(ctx, entity.GetFullEntityPath())
	if err != nil {
		return diag.Diagnostics{{Severity: diag.Warning, Summary: "IAM EDS saved; refresh required", Detail: "Read-back failed: " + iamStatus(err) + ". The acknowledged EDS identity is retained."}}
	}
	return diag.FromErr(flattenDescription(d, entity, desc.Properties, desc.SourceType, desc.Location))
}

// A lost response does not establish whether the server has stopped using the secret.
func iamOutcomeUnknown(err error) bool {
	if err == nil || ydb.IsOperationError(err, Ydb.StatusIds_BAD_REQUEST, Ydb.StatusIds_UNAUTHORIZED,
		Ydb.StatusIds_PRECONDITION_FAILED, Ydb.StatusIds_ALREADY_EXISTS, Ydb.StatusIds_NOT_FOUND,
		Ydb.StatusIds_SCHEME_ERROR, Ydb.StatusIds_UNSUPPORTED) {
		return false
	}
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated, codes.InvalidArgument, codes.AlreadyExists,
		codes.NotFound, codes.FailedPrecondition, codes.Unimplemented:
		return false
	}
	return true
}

// Server messages may contain credentials; keep only their status.
func iamStatus(err error) string {
	if e := ydb.OperationError(err); e != nil {
		return fmt.Sprintf("YDB status %d", e.Code())
	}
	if e := ydb.TransportError(err); e != nil {
		return fmt.Sprintf("transport status %d", e.Code())
	}
	return "request failed (credential details omitted)"
}
