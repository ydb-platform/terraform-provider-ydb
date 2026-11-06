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

// IAM Create must not replace or acquire an existing EDS.
func TestAccYdbExternalDataSource_iamCreatePreservesExisting(t *testing.T) {
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
	parametersSupported := accIAMSecretParametersSupported(t, db, p+"_parameter_probe")
	secrets := func() map[string]bool {
		t.Helper()
		paths, err := accIAMSecretPaths(ctx, db, root)
		require.NoError(t, err)
		return paths
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

	after := secrets()
	switch {
	case parametersSupported && strings.Contains(fmt.Sprint(diags), "outcome is unknown"):
		var retained []string
		for name := range after {
			if !before[name] {
				retained = append(retained, name)
			}
		}
		require.Len(t, retained, 1)
		owned := path.Join(root, retained[0])
		require.Contains(t, fmt.Sprint(diags), strconv.Quote(owned))
		t.Cleanup(func() {
			require.NoError(t, db.Query().Exec(context.WithoutCancel(ctx), "DROP SECRET `"+helpers.EscapeYQLIdentifier(owned)+"`"))
		})
		// YDB reports this collision as GENERIC_ERROR; the provider retains the secret conservatively.
		require.Contains(t, fmt.Sprint(diags), fmt.Sprintf("IAM EDS outcome is unknown (YDB status %d)", Ydb.StatusIds_GENERIC_ERROR))
		delete(after, retained[0])
	case parametersSupported:
		require.Contains(t, fmt.Sprint(diags), "IAM EDS request failed")
	default:
		require.Contains(t, fmt.Sprint(diags), "temporary IAM secret CREATE was not acknowledged")
		require.Contains(t, fmt.Sprint(diags), "EDS was not submitted", "unsupported servers must fail without a literal fallback")
	}
	require.True(t, diags.HasError())
	require.NotContains(t, fmt.Sprint(diags), token)
	require.Empty(t, d.Id(), "a rejected Create must not acquire the existing EDS")
	desc, err := db.Table().DescribeExternalDataSource(ctx, p)
	require.NoError(t, err)
	require.Equal(t, "NONE", desc.Properties["AUTH_METHOD"])
	require.Equal(t, location, desc.Location)
	require.Equal(t, before, after, "only the diagnosed owned secret may be retained")
}

func accIAMSecretParametersSupported(t *testing.T, db *ydb.Driver, probe string) bool {
	t.Helper()
	ctx := t.Context()
	err := db.Query().Exec(ctx,
		"DECLARE $value AS Utf8; CREATE SECRET `"+helpers.EscapeYQLIdentifier(probe)+"` WITH (value = $value)",
		query.WithParameters(ydb.ParamsBuilder().Param("$value").Text("public parameter capability probe").Build()),
	)
	if err == nil {
		require.NoError(t, db.Query().Exec(ctx, "DROP SECRET `"+helpers.EscapeYQLIdentifier(probe)+"`"))
		return true
	}
	require.True(t, ydb.IsOperationError(err, Ydb.StatusIds_GENERIC_ERROR), "unexpected probe failure: %v", err)
	require.Contains(t, err.Error(), "Expected data type: String, but got: Utf8")
	return false
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
