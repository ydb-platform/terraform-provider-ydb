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
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
	eds "github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/externaldatasource"
)

// IAM Create preserves an existing EDS and cleans up its owned bootstrap secret.
func TestAccYdbExternalDataSource_iamBootstrapCleanup(t *testing.T) {
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
	probe := p + "_parameter_probe"
	probeErr := db.Query().Exec(ctx,
		"DECLARE $value AS Utf8; CREATE SECRET `"+helpers.EscapeYQLIdentifier(probe)+"` WITH (value = $value)",
		query.WithParameters(ydb.ParamsBuilder().Param("$value").Text("public parameter capability probe").Build()),
	)
	parametersSupported := probeErr == nil
	if parametersSupported {
		require.NoError(t, db.Query().Exec(ctx, "DROP SECRET `"+helpers.EscapeYQLIdentifier(probe)+"`"))
	} else {
		require.True(t, ydb.IsOperationError(probeErr, Ydb.StatusIds_GENERIC_ERROR), "unexpected probe failure: %v", probeErr)
		require.Contains(t, probeErr.Error(), "Expected data type: String, but got: Utf8")
	}
	secrets := func() map[string]bool {
		t.Helper()
		dir, err := db.Scheme().ListDirectory(ctx, root)
		require.NoError(t, err)
		result := map[string]bool{}
		for _, child := range dir.Children {
			if strings.HasPrefix(child.Name, "__tf_iam_") {
				result[child.Name] = true
			}
		}
		return result
	}
	before := secrets()
	d := schema.TestResourceDataRaw(t, eds.ResourceSchema(), map[string]interface{}{
		"connection_string": conn, "path": name, "source_type": "Ydb",
		"location": location, "database_name": root, "auth_method": "IAM",
		"service_account_id": "synthetic-runtime-sa",
	})
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: token}, nil
	}

	diags := eds.ResourceCreateFunc(cb)(ctx, d, nil)

	require.True(t, diags.HasError())
	if parametersSupported {
		require.Contains(t, fmt.Sprint(diags), "IAM EDS request failed", "bootstrap secret creation must succeed before the terminal EDS rejection")
	} else {
		require.Contains(t, fmt.Sprint(diags), "temporary IAM secret CREATE was not acknowledged")
		require.Contains(t, fmt.Sprint(diags), "EDS was not submitted", "unsupported servers must fail without a literal fallback")
	}
	require.NotContains(t, fmt.Sprint(diags), token)
	require.Empty(t, d.Id(), "a rejected Create must not acquire the existing EDS")
	desc, err := db.Table().DescribeExternalDataSource(ctx, p)
	require.NoError(t, err)
	require.Equal(t, "NONE", desc.Properties["AUTH_METHOD"])
	require.Equal(t, location, desc.Location)
	require.Equal(t, before, secrets(), "terminal EDS rejection must leave no temporary caller-token secret")
}
