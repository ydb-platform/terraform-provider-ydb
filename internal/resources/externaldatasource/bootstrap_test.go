package externaldatasource

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func bootstrapData(t *testing.T) *schema.ResourceData {
	t.Helper()
	s := flattenTestSchema()
	for _, k := range []string{"iam_bootstrap_mode", "initial_token_secret_path", "resource_id", "pending_cleanup_path", "pending_iam_mutation"} {
		s[k] = &schema.Schema{Type: schema.TypeString, Optional: true}
	}
	return schema.TestResourceDataRaw(t, s, map[string]interface{}{"connection_string": "grpc://localhost:2136/?database=/owner", "path": "eds", "source_type": "Ydb", "location": "target:2135", "auth_method": "IAM", "service_account_id": "runtime-sa", "database_id": "target", "iam_bootstrap_mode": "CALLER_TOKEN", "use_tls": false})
}

func bootstrapHandler(f *fakeBackend) *Handler {
	h := NewHandlerWithCallerAuth(auth.YdbCredentials{Token: "transport"}, func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: "SENTINEL-caller-'\\\n"}, nil
	})
	h.openBackend = func(context.Context, string, auth.YdbCredentials) (backend, error) { return f, nil }
	h.uniqueName = func() string { return "__tf_iam_bootstrap_unique" }
	return h
}

func iamDescription() options.ExternalDataSourceDescription {
	return options.ExternalDataSourceDescription{SourceType: "Ydb", Location: "target:2135", Properties: map[string]string{"AUTH_METHOD": "IAM", "SERVICE_ACCOUNT_ID": "runtime-sa", "DATABASE_ID": "target", "INITIAL_TOKEN_SECRET_PATH": "/owner/__tf_iam_bootstrap_unique", "RESOURCE_ID": "target-cloud", "USE_TLS": "FALSE"}}
}

// The break caught is overwriting a foreign IAM EDS or omitting the temporary caller secret.
func TestYDBIAMBootstrapSequence(t *testing.T) {
	f := &fakeBackend{desc: iamDescription()}
	h := bootstrapHandler(f)
	d := bootstrapData(t)
	diags := h.Create(context.Background(), d, nil)
	require.False(t, diags.HasError(), "%v", diags)
	require.Len(t, f.queries, 3)
	require.Contains(t, f.queries[0], "DECLARE $value AS Utf8;")
	require.Contains(t, f.queries[0], "CREATE SECRET `/owner/__tf_iam_bootstrap_unique` WITH (value = $value)")
	require.True(t, strings.HasPrefix(f.queries[1], "CREATE EXTERNAL DATA SOURCE "), f.queries[1])
	require.Contains(t, f.queries[1], `INITIAL_TOKEN_SECRET_PATH = "/owner/__tf_iam_bootstrap_unique"`)
	require.Contains(t, f.queries[2], "DROP SECRET `/owner/__tf_iam_bootstrap_unique`")
	require.NotEmpty(t, d.Id())
	require.Equal(t, "", d.Get("pending_cleanup_path"))
	require.False(t, strings.Contains(strings.Join(f.queries, "\n"), "SENTINEL"), "SQL exposed bootstrap token")
}

func TestYDBIAMApplyValidation(t *testing.T) {
	f := &fakeBackend{}
	h := bootstrapHandler(f)
	d := bootstrapData(t)
	require.NoError(t, d.Set("service_account_id", ""))
	diags := h.Create(context.Background(), d, nil)
	require.True(t, diags.HasError())
	require.Empty(t, f.queries)
}

func TestYDBIAMBootstrapTerminalCleanup(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		mutationErr, readErr error
	}{
		{"success", nil, nil}, {"terminal failure", status.Error(codes.PermissionDenied, "terminal failure"), nil}, {"Describe failure", nil, errors.New("describe failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeBackend{desc: iamDescription(), describeErr: tc.readErr}
			f.exec = func(_ context.Context, q string, _ []query.ExecuteOption) error {
				if strings.Contains(q, "EXTERNAL DATA SOURCE") {
					return tc.mutationErr
				}
				return nil
			}
			h := bootstrapHandler(f)
			d := bootstrapData(t)
			diags := h.Create(context.Background(), d, nil)
			if tc.mutationErr != nil {
				require.True(t, diags.HasError())
				require.Empty(t, d.Id())
			}
			require.Len(t, f.queries, 3)
			require.Contains(t, f.queries[2], "DROP SECRET")
		})
	}
}

func TestYDBIAMBootstrapCleanupRetry(t *testing.T) {
	f := &fakeBackend{desc: iamDescription()}
	fail := true
	f.exec = func(_ context.Context, q string, _ []query.ExecuteOption) error {
		if strings.HasPrefix(q, "DROP SECRET") && fail {
			return status.Error(codes.PermissionDenied, "no permission")
		}
		return nil
	}
	h := bootstrapHandler(f)
	d := bootstrapData(t)
	diags := h.Create(context.Background(), d, nil)
	require.False(t, diags.HasError())
	require.NotEmpty(t, d.Id())
	require.Equal(t, "/owner/__tf_iam_bootstrap_unique", d.Get("pending_cleanup_path"))
	require.NotEmpty(t, diags)
	originalID := d.Id()
	diags = h.Read(context.Background(), d, nil)
	require.True(t, diags.HasError())
	require.Equal(t, originalID, d.Id())
	require.NotEmpty(t, d.Get("pending_cleanup_path"))
	fail = false
	diags = h.Read(context.Background(), d, nil)
	require.False(t, diags.HasError())
	require.Equal(t, "", d.Get("pending_cleanup_path"))
	require.Equal(t, originalID, d.Id())
	require.Len(t, f.queries, 5)
	for _, q := range f.queries[2:] {
		require.True(t, strings.HasPrefix(q, "DROP SECRET"), q)
	}
}

func TestYDBIAMBootstrapCollisionAndUpdate(t *testing.T) {
	f := &fakeBackend{desc: iamDescription()}
	names := []string{"collision", "owned-one", "owned-two"}
	next := 0
	f.exec = func(_ context.Context, q string, _ []query.ExecuteOption) error {
		if strings.Contains(q, "CREATE SECRET `/owner/collision`") {
			return status.Error(codes.AlreadyExists, "exists")
		}
		return nil
	}
	h := bootstrapHandler(f)
	h.uniqueName = func() string { s := names[next]; next++; return s }
	d := bootstrapData(t)
	require.False(t, h.Create(context.Background(), d, nil).HasError())
	require.False(t, h.Update(context.Background(), d, nil).HasError())
	require.Len(t, f.queries, 7)
	require.Contains(t, f.queries[1], "CREATE SECRET `/owner/owned-one`")
	require.Contains(t, f.queries[4], "CREATE SECRET `/owner/owned-two`")
	require.Contains(t, f.queries[5], "CREATE OR REPLACE EXTERNAL DATA SOURCE")
	for _, q := range f.queries {
		require.NotContains(t, q, "DROP SECRET `/owner/collision`")
		require.NotContains(t, q, "ALTER SECRET")
	}
}

type traceKey struct{}

func TestYDBIAMBootstrapCancellation(t *testing.T) {
	f := &fakeBackend{desc: iamDescription()}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), traceKey{}, "retained"))
	defer cancel()
	f.exec = func(c context.Context, q string, _ []query.ExecuteOption) error {
		if strings.Contains(q, "EXTERNAL DATA SOURCE") {
			cancel()
		}
		if strings.HasPrefix(q, "DROP SECRET") {
			require.NoError(t, c.Err())
			_, ok := c.Deadline()
			require.True(t, ok)
			require.Equal(t, "retained", c.Value(traceKey{}))
		}
		return nil
	}
	h := bootstrapHandler(f)
	h.callerAuth = func(c context.Context) (auth.YdbCredentials, error) {
		require.NoError(t, c.Err())
		return auth.YdbCredentials{Token: "caller"}, nil
	}
	d := bootstrapData(t)
	require.False(t, h.Create(ctx, d, nil).HasError())
	require.Len(t, f.queries, 3)
	f.queries = nil
	d = bootstrapData(t)
	require.True(t, h.Create(ctx, d, nil).HasError())
	require.Empty(t, f.queries)
}

func TestYDBIAMBootstrapUnknownOutcomes(t *testing.T) {
	for _, phase := range []string{"secret", "create EDS", "update EDS"} {
		t.Run(phase, func(t *testing.T) {
			f := &fakeBackend{desc: iamDescription()}
			f.exec = func(_ context.Context, q string, _ []query.ExecuteOption) error {
				if (phase == "secret" && strings.Contains(q, "CREATE SECRET")) || (phase != "secret" && strings.Contains(q, "EXTERNAL DATA SOURCE")) {
					return status.Error(codes.DeadlineExceeded, "response lost")
				}
				return nil
			}
			h := bootstrapHandler(f)
			d := bootstrapData(t)
			var diags interface{ HasError() bool }
			if phase == "update EDS" {
				d.SetId("grpc://localhost:2136/?database=/owner?path=eds")
				diags = h.Update(context.Background(), d, nil)
				require.NotEmpty(t, d.Id())
				require.Equal(t, "UNKNOWN", d.Get("pending_iam_mutation"))
				require.NotEmpty(t, d.Get("pending_cleanup_path"))
				before := len(f.queries)
				require.True(t, h.Read(context.Background(), d, nil).HasError())
				require.Len(t, f.queries, before)
			} else {
				diags = h.Create(context.Background(), d, nil)
				require.Empty(t, d.Id())
			}
			require.True(t, diags.HasError())
			// An unknown result must return without a Describe that cannot prove
			// ownership or termination, delaying the operator-facing diagnostic.
			require.Zero(t, f.describeCalls)
			for _, q := range f.queries {
				require.NotContains(t, q, "DROP SECRET")
			}
		})
	}
}

func TestYDBIAMBootstrapNoSensitiveOutput(t *testing.T) {
	const token = "SENTINEL-caller-'\\\n"
	for _, phase := range []string{"token", "secret", "mutation", "cleanup", "read"} {
		t.Run(phase, func(t *testing.T) {
			f := &fakeBackend{desc: iamDescription()}
			failure := status.Error(codes.PermissionDenied, "credential="+token)
			f.exec = func(_ context.Context, q string, _ []query.ExecuteOption) error {
				if (phase == "secret" && strings.Contains(q, "CREATE SECRET")) || (phase == "mutation" && strings.Contains(q, "EXTERNAL DATA SOURCE")) || (phase == "cleanup" && strings.HasPrefix(q, "DROP SECRET")) {
					return failure
				}
				return nil
			}
			if phase == "read" {
				f.describeErr = failure
			}
			h := bootstrapHandler(f)
			if phase == "token" {
				h.callerAuth = func(context.Context) (auth.YdbCredentials, error) { return auth.YdbCredentials{}, failure }
			}
			d := bootstrapData(t)
			diags := h.Create(context.Background(), d, nil)
			serialized := fmt.Sprintf("%v %v", diags, d.State())
			require.False(t, strings.Contains(serialized, token), "credential escaped into diagnostics/state")
			require.False(t, strings.Contains(strings.Join(f.queries, ""), token), "credential escaped into SQL")
		})
	}
}

func TestYDBIAMBootstrapExternalAndImport(t *testing.T) {
	f := &fakeBackend{desc: iamDescription()}
	h := bootstrapHandler(f)
	d := bootstrapData(t)
	require.NoError(t, d.Set("iam_bootstrap_mode", "EXISTING_SECRET"))
	require.NoError(t, d.Set("initial_token_secret_path", "/owner/external"))
	require.False(t, h.Create(context.Background(), d, nil).HasError())
	require.False(t, h.Update(context.Background(), d, nil).HasError())
	require.False(t, h.Read(context.Background(), d, nil).HasError())
	for _, q := range f.queries {
		require.NotContains(t, q, "CREATE SECRET")
		require.NotContains(t, q, "DROP SECRET")
	}
	d = bootstrapData(t)
	d.SetId("grpc://localhost:2136/?database=/owner?path=eds")
	require.NoError(t, d.Set("iam_bootstrap_mode", ""))
	before := len(f.queries)
	require.False(t, h.Read(context.Background(), d, nil).HasError())
	require.Equal(t, "EXISTING_SECRET", d.Get("iam_bootstrap_mode"))
	require.Len(t, f.queries, before)
}

func TestYDBIAMBootstrapFailedCreateCleanupOrphan(t *testing.T) {
	f := &fakeBackend{}
	f.exec = func(_ context.Context, q string, _ []query.ExecuteOption) error {
		if strings.Contains(q, "EXTERNAL DATA SOURCE") || strings.HasPrefix(q, "DROP SECRET") {
			return status.Error(codes.PermissionDenied, "rejected")
		}
		return nil
	}
	h := bootstrapHandler(f)
	d := bootstrapData(t)
	diags := h.Create(context.Background(), d, nil)
	require.True(t, diags.HasError())
	require.Empty(t, d.Id())
	require.Contains(t, fmt.Sprint(diags), "confirmed-owned orphan")
	require.Contains(t, fmt.Sprint(diags), "/owner/__tf_iam_bootstrap_unique")
}

func TestYDBIAMBootstrapCleanupDeadline(t *testing.T) {
	f := &fakeBackend{desc: iamDescription()}
	f.exec = func(ctx context.Context, q string, _ []query.ExecuteOption) error {
		if strings.HasPrefix(q, "DROP SECRET") {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	h := bootstrapHandler(f)
	h.cleanupTimeout = 10 * time.Millisecond
	d := bootstrapData(t)
	started := time.Now()
	diags := h.Create(context.Background(), d, nil)
	require.False(t, diags.HasError())
	require.NotEmpty(t, d.Get("pending_cleanup_path"))
	require.Less(t, time.Since(started), time.Second)
}

func TestYDBIAMBootstrapConcurrentPaths(t *testing.T) {
	results := make(chan bootstrapResult, 2)
	for i := 0; i < 2; i++ {
		go func() {
			f := &fakeBackend{}
			h := bootstrapHandler(f)
			h.uniqueName = nil
			results <- h.bootstrap(context.Background(), "grpc://localhost:2136/?database=/owner", "/owner", f, func(string) mutationResult { return mutationResult{outcome: terminalSuccess} })
		}()
	}
	first, second := <-results, <-results
	require.Equal(t, terminalSuccess, first.outcome)
	require.Equal(t, terminalSuccess, second.outcome)
	require.NotEqual(t, first.path, second.path)
}

func TestMissingCallerTokenDiagnostic(t *testing.T) {
	f := &fakeBackend{}
	h := bootstrapHandler(f)
	h.callerAuth = func(context.Context) (auth.YdbCredentials, error) { return auth.YdbCredentials{}, nil }
	diags := h.Create(context.Background(), bootstrapData(t), nil)
	require.True(t, diags.HasError())
	require.Contains(t, diags[0].Summary, "caller IAM token required")
	require.Empty(t, f.queries)
}

func TestUnknownSecretCreateDuringUpdateDoesNotBlockEDS(t *testing.T) {
	f := &fakeBackend{desc: iamDescription()}
	f.exec = func(_ context.Context, q string, _ []query.ExecuteOption) error {
		if strings.Contains(q, "CREATE SECRET") {
			return status.Error(codes.DeadlineExceeded, "response lost")
		}
		return nil
	}
	h := bootstrapHandler(f)
	d := bootstrapData(t)
	d.SetId("grpc://localhost:2136/?database=/owner?path=eds")
	diags := h.Update(context.Background(), d, nil)
	require.True(t, diags.HasError())
	require.Contains(t, diags[0].Summary, "Secret CREATE was not acknowledged")
	require.NotContains(t, diags[0].Summary, "EDS mutation outcome is unknown")
	require.Empty(t, d.Get("pending_iam_mutation"))
	require.Empty(t, d.Get("pending_cleanup_path"))
	require.Len(t, f.queries, 1)
	require.False(t, h.Read(context.Background(), d, nil).HasError())
}
