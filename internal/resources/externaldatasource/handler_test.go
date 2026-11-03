package externaldatasource

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

type legacyBackend struct {
	queries              []string
	paths                []string
	execErr, describeErr error
	closed               int
}

func (b *legacyBackend) Exec(_ context.Context, sql string, _ ...query.ExecuteOption) error {
	b.queries = append(b.queries, sql)
	return b.execErr
}

func (b *legacyBackend) Describe(_ context.Context, path string) (options.ExternalDataSourceDescription, error) {
	b.paths = append(b.paths, path)
	return options.ExternalDataSourceDescription{SourceType: "Ydb", Location: "observed:2135", Properties: map[string]string{"AUTH_METHOD": "NONE", "DATABASE_ID": "target"}}, b.describeErr
}
func (b *legacyBackend) PathAbsent(context.Context, string) (bool, error) { return false, nil }

func (b *legacyBackend) Close(context.Context) error { b.closed++; return nil }

func legacyData(t *testing.T) *schema.ResourceData {
	t.Helper()
	return schema.TestResourceDataRaw(t, flattenTestSchema(), map[string]interface{}{
		"connection_string": "grpc://localhost:2136/?database=/owner", "path": "eds", "source_type": "Ydb", "location": "target:2135", "auth_method": "NONE", "database_id": "target",
	})
}

func legacyHandler(b *legacyBackend) *Handler {
	h := NewHandler(auth.YdbCredentials{Token: "transport"})
	h.openBackend = func(context.Context, string, auth.YdbCredentials) (backend, error) { return b, nil }
	return h
}

// Catch replacing CREATE OR REPLACE with strict CREATE, dropping read-back, or
// deriving an Update's path/endpoint from mutable config instead of its identity.
func TestLegacyHandlerMutation(t *testing.T) {
	for _, update := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "update"}[update], func(t *testing.T) {
			b := &legacyBackend{}
			h, d := legacyHandler(b), legacyData(t)
			wantID := "grpc://localhost:2136/?database=/owner?path=eds"
			wantPath := "/owner/eds"
			if update {
				wantID = "grpc://localhost:2136/?database=/original?path=saved"
				wantPath = "/original/saved"
				d.SetId(wantID)
			}
			var endpoints []string
			h.openBackend = func(_ context.Context, endpoint string, creds auth.YdbCredentials) (backend, error) {
				require.Equal(t, "transport", creds.Token)
				endpoints = append(endpoints, endpoint)
				return b, nil
			}
			var diags interface{}
			if update {
				diags = h.Update(context.Background(), d, nil)
			} else {
				diags = h.Create(context.Background(), d, nil)
			}
			require.Empty(t, diags)
			require.Equal(t, wantID, d.Id())
			require.Len(t, b.queries, 1)
			require.True(t, strings.HasPrefix(b.queries[0], "CREATE OR REPLACE EXTERNAL DATA SOURCE `"+wantPath+"`"), b.queries[0])
			require.Contains(t, b.queries[0], `AUTH_METHOD = "NONE"`)
			require.Contains(t, b.queries[0], `LOCATION = "target:2135"`)
			require.Equal(t, []string{wantPath}, b.paths)
			require.Equal(t, "observed:2135", d.Get("location"))
			require.Positive(t, b.closed)
			require.NotEmpty(t, endpoints)
			for _, endpoint := range endpoints {
				require.Equal(t, endpoints[0], endpoint)
			}
			if update {
				require.Contains(t, endpoints[0], "database=/original")
			}
		})
	}
}

func TestLegacyHandlerMutationFailures(t *testing.T) {
	for _, update := range []bool{false, true} {
		for _, stage := range []string{"open", "execute", "describe"} {
			t.Run(map[bool]string{false: "create", true: "update"}[update]+"/"+stage, func(t *testing.T) {
				b := &legacyBackend{}
				h, d := legacyHandler(b), legacyData(t)
				originalID := ""
				if update {
					originalID = "grpc://localhost:2136/?database=/owner?path=eds"
					d.SetId(originalID)
				}
				switch stage {
				case "open":
					h.openBackend = func(context.Context, string, auth.YdbCredentials) (backend, error) {
						return nil, errors.New("unavailable")
					}
				case "execute":
					b.execErr = errors.New("unavailable")
				case "describe":
					b.describeErr = errors.New("unavailable")
				}
				if update {
					require.True(t, h.Update(context.Background(), d, nil).HasError())
				} else {
					require.True(t, h.Create(context.Background(), d, nil).HasError())
				}
				if !update && stage == "describe" {
					originalID = "grpc://localhost:2136/?database=/owner?path=eds"
				}
				require.Equal(t, originalID, d.Id())
				if stage == "open" {
					require.Empty(t, b.queries)
					require.Zero(t, b.closed)
				} else {
					require.Len(t, b.queries, 1)
					require.Positive(t, b.closed)
				}
			})
		}
	}
}

// Invalid source/auth combinations and missing Update identity must fail before I/O.
func TestLegacyHandlerValidation(t *testing.T) {
	for _, update := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "update"}[update], func(t *testing.T) {
			h, d := legacyHandler(&legacyBackend{}), legacyData(t)
			h.openBackend = func(context.Context, string, auth.YdbCredentials) (backend, error) {
				t.Fatal("invalid mutation reached database")
				return nil, nil
			}
			if update {
				require.True(t, h.Update(context.Background(), d, nil).HasError())
				return
			}
			require.NoError(t, d.Set("auth_method", "AWS"))
			require.True(t, h.Create(context.Background(), d, nil).HasError())
			require.Empty(t, d.Id())
		})
	}
}
