package terraform_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	eds "github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/externaldatasource"
)

func TestAccYdbExternalDataSource_iamCreateReplacesExisting(t *testing.T) {
	if os.Getenv("TF_ACC") != "1" {
		t.Skip("set TF_ACC=1 to run acceptance tests")
	}
	accPreCheckYDB(t)
	conn := os.Getenv(envAccYDBConnection)
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
	name := "__tf_acc_iam_local_" + accRandomHex8(t)
	p := path.Join(root, name)
	location := accLocationHostPortFromConn(conn)
	q := fmt.Sprintf("CREATE EXTERNAL DATA SOURCE `%s` WITH (SOURCE_TYPE = \"Ydb\", LOCATION = %s, AUTH_METHOD = \"NONE\", DATABASE_NAME = %s)", helpers.EscapeYQLIdentifier(p), strconv.Quote(location), strconv.Quote(root))
	require.NoError(t, db.Query().Exec(ctx, q))
	t.Cleanup(func() {
		err := db.Query().Exec(context.WithoutCancel(ctx), "DROP EXTERNAL DATA SOURCE `"+helpers.EscapeYQLIdentifier(p)+"`")
		require.NoError(t, err)
	})
	secrets := func() map[string]bool {
		t.Helper()
		paths, err := accIAMSecretPaths(ctx, db, root)
		require.NoError(t, err)
		return paths
	}
	before := secrets()
	t.Cleanup(func() {
		after, err := accIAMSecretPaths(context.WithoutCancel(ctx), db, root)
		require.NoError(t, err)
		for name := range after {
			if !before[name] {
				err := db.Query().Exec(context.WithoutCancel(ctx), "DROP SECRET `"+helpers.EscapeYQLIdentifier(path.Join(root, name))+"`")
				require.NoError(t, err)
			}
		}
	})
	d := schema.TestResourceDataRaw(t, eds.ResourceSchema(), map[string]interface{}{
		"connection_string": conn, "path": name, "source_type": "Ydb",
		"location": location, "database_name": root, "auth_method": "IAM",
		"service_account_id": "synthetic-runtime-sa",
	})
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: token}, nil
	}

	diags := eds.ResourceCreateFunc(cb)(ctx, d, nil)

	require.False(t, diags.HasError(), "%v", diags)
	require.NotContains(t, fmt.Sprint(diags), token)
	require.Equal(t, conn+"?path="+name, d.Id())
	desc, err := db.Table().DescribeExternalDataSource(ctx, p)
	require.NoError(t, err)
	require.Equal(t, "IAM", desc.Properties["AUTH_METHOD"])
	require.Equal(t, "synthetic-runtime-sa", desc.Properties["SERVICE_ACCOUNT_ID"])
	require.Equal(t, location, desc.Location)
	require.Equal(t, before, secrets(), "replacing an EDS must remove its temporary IAM secret")
}

func accIAMSecretPaths(ctx context.Context, db *ydb.Driver, root string) (map[string]bool, error) {
	dir, err := db.Scheme().ListDirectory(ctx, root)
	if err != nil {
		return nil, err
	}
	paths := map[string]bool{}
	for _, child := range dir.Children {
		if strings.HasPrefix(child.Name, "__tf_iam_") {
			paths[child.Name] = true
		}
	}
	return paths, nil
}
