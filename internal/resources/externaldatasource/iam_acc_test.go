package externaldatasource

import (
	"context"
	"net/url"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

// TestAccIAMSecret checks the actual server's secret DDL and catalog lifecycle.
func TestAccIAMSecret(t *testing.T) {
	if os.Getenv("TF_ACC") != "1" {
		t.Skip("set TF_ACC=1 and YDB_ACC_CONNECTION_STRING for local YDB acceptance")
	}
	conn := os.Getenv("YDB_ACC_CONNECTION_STRING")
	require.NotEmpty(t, conn)
	u, err := url.Parse(conn)
	require.NoError(t, err)
	root := u.Query().Get("database")
	require.NotEmpty(t, root)
	token := os.Getenv("YDB_ACC_TOKEN")
	if token == "" {
		token = "synthetic-local-'\"\\-iam-caller"
	}
	ctx := t.Context()
	db, err := ydb.Open(ctx, conn, ydb.WithAccessTokenCredentials(token))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(context.WithoutCancel(ctx)) })
	h := NewHandler(auth.YdbCredentials{Token: token})
	p, err := h.createIAMSecret(ctx, db, root)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropIAMSecret(context.WithoutCancel(ctx), db, p) })
	dir, err := db.Scheme().ListDirectory(ctx, root)
	require.NoError(t, err)
	found := false
	for _, child := range dir.Children {
		found = found || child.Name == path.Base(p)
	}
	require.True(t, found, "acknowledged secret must exist in the real catalog")
	require.NoError(t, dropIAMSecret(ctx, db, p))
	dir, err = db.Scheme().ListDirectory(ctx, root)
	require.NoError(t, err)
	for _, child := range dir.Children {
		require.NotEqual(t, path.Base(p), child.Name)
	}
}
