package externaldatasource

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func TestIAMCancelledBeforeSubmissionRetainsKnownOutcome(t *testing.T) {
	d := schema.TestResourceDataRaw(t, map[string]*schema.Schema{
		"pending_cleanup_path": {Type: schema.TypeString, Computed: true},
		"pending_iam_mutation": {Type: schema.TypeString, Computed: true},
	}, nil)
	d.SetId("owned-eds")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	diags := cancelIAMSubmission(ctx, d, nil, "", time.Minute)

	require.True(t, diags.HasError())
	require.Equal(t, "owned-eds", d.Id())
	require.Empty(t, d.Get("pending_iam_mutation"), "a request that was not submitted cannot have an unknown server outcome")
	require.Empty(t, d.Get("pending_cleanup_path"))
	require.Contains(t, diags[0].Summary, "not submitted")
}

func TestAccIAMCancelledBeforeSubmissionCleansOwnedSecret(t *testing.T) {
	if os.Getenv("TF_ACC") != "1" {
		t.Skip("set TF_ACC=1 and YDB_ACC_CONNECTION_STRING for local YDB acceptance")
	}
	conn := os.Getenv("YDB_ACC_CONNECTION_STRING")
	require.NotEmpty(t, conn)
	u, err := url.Parse(conn)
	require.NoError(t, err)
	root := u.Query().Get("database")
	require.NotEmpty(t, root)
	const token = "synthetic-local-cancelled-caller"
	db, err := ydb.Open(t.Context(), conn, ydb.WithAccessTokenCredentials(token))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(context.WithoutCancel(t.Context())) })
	h := NewHandler(auth.YdbCredentials{Token: token})
	p, err := h.createIAMSecret(t.Context(), db, root)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropIAMSecret(context.WithoutCancel(t.Context()), db, p) })
	d := schema.TestResourceDataRaw(t, map[string]*schema.Schema{
		"pending_cleanup_path": {Type: schema.TypeString, Computed: true},
		"pending_iam_mutation": {Type: schema.TypeString, Computed: true},
	}, nil)
	d.SetId("owned-eds")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	diags := cancelIAMSubmission(ctx, d, db, p, d.Timeout(schema.TimeoutUpdate))

	require.True(t, diags.HasError())
	require.Equal(t, "owned-eds", d.Id())
	require.Empty(t, d.Get("pending_iam_mutation"), "EDS was never submitted")
	require.Empty(t, d.Get("pending_cleanup_path"))
	absent, err := iamPathAbsent(t.Context(), db, p)
	require.NoError(t, err)
	require.True(t, absent, "cancellation before submission must remove the acknowledged-owned bootstrap secret")
}
