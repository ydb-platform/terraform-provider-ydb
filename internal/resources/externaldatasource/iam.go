package externaldatasource

import (
	"context"
	"crypto/rand"
	"fmt"
	"path"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/terraform-provider-ydb/internal/resources/secret"
)

func (h *Handler) createIAMSecret(ctx context.Context, db *ydb.Driver, root string) (string, error) {
	if h.authCreds.Token == "" {
		return "", fmt.Errorf("IAM authentication requires the caller's IAM token")
	}
	p := path.Join(root, "__tf_iam_"+rand.Text())
	if err := secret.CreateSecret(ctx, db, p, h.authCreds.Token, false); err != nil {
		return "", fmt.Errorf("failed to create temporary IAM secret %q: %w", p, err)
	}
	return p, nil
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
