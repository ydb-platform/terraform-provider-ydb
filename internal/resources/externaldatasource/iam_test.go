package externaldatasource

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

func TestIAMAuthValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields map[string]string
		plan   testAuthPlan
		bad    bool
	}{
		{"caller", map[string]string{}, nil, false},
		{"existing", map[string]string{"iam_bootstrap_mode": "EXISTING_SECRET", "initial_token_secret_path": "secrets/caller"}, nil, false},
		{"existing missing", map[string]string{"iam_bootstrap_mode": "EXISTING_SECRET"}, nil, true},
		{"SA missing", map[string]string{"service_account_id": ""}, nil, true},
		{"SA unknown", map[string]string{"service_account_id": ""}, testAuthPlan{"service_account_id": false}, false},
		{"path unknown", map[string]string{"iam_bootstrap_mode": "EXISTING_SECRET"}, testAuthPlan{"initial_token_secret_path": false}, false},
		{"bad mode", map[string]string{"iam_bootstrap_mode": "BAD"}, nil, true},
		{"forbidden password", map[string]string{"password_secret_path": "secret"}, nil, true},
		{"forbidden token", map[string]string{"token_secret_path": "secret"}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vals := map[string]string{"auth_method": "IAM", "source_type": "Ydb", "service_account_id": "runtime-sa", "database_id": "target"}
			for k, v := range tc.fields {
				vals[k] = v
			}
			var plan resourcePlanDiff
			if tc.plan != nil {
				plan = tc.plan
			}
			err := validateResourceAuth(res(vals), plan)
			if tc.bad {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NoError(t, validateSourceType(res(vals), plan))
			}
		})
	}
	require.Error(t, validateSourceType(res(map[string]string{"source_type": "ObjectStorage", "auth_method": "IAM"}), nil))
}

func TestIAMMutationSQL(t *testing.T) {
	r := res(map[string]string{"source_type": "Ydb", "location": "target:2135", "auth_method": "IAM", "service_account_id": "sa", "initial_token_secret_path": "/owner/temporary", "resource_id": "target-cloud"})
	q := PrepareDataSourceQuery("/owner/eds", r)
	require.Contains(t, q, `INITIAL_TOKEN_SECRET_PATH = "/owner/temporary"`)
	require.Contains(t, q, `RESOURCE_ID = "target-cloud"`)
	r.Values["service_account_id"] = "s'a\\b\"c"
	q = PrepareDataSourceQuery("/owner/a`b", r)
	require.Contains(t, q, "`/owner/a``b`")
	require.Contains(t, q, `SERVICE_ACCOUNT_ID = "s'a\\b\"c"`)
}

func TestIAMFlattenRoundTrip(t *testing.T) {
	s := flattenTestSchema()
	for _, k := range []string{"initial_token_secret_path", "resource_id", "iam_bootstrap_mode"} {
		s[k] = &schema.Schema{Type: schema.TypeString, Optional: true}
	}
	d := schema.TestResourceDataRaw(t, s, map[string]interface{}{"use_tls": false, "iam_bootstrap_mode": "CALLER_TOKEN"})
	entity, err := helpers.ParseYDBEntityID("grpc://localhost:2136/?database=/owner?path=eds")
	require.NoError(t, err)
	require.NoError(t, flattenDescription(d, entity, map[string]string{"AUTH_METHOD": "IAM", "SERVICE_ACCOUNT_ID": "sa", "INITIAL_TOKEN_SECRET_PATH": "/owner/tmp", "RESOURCE_ID": "cloud", "USE_TLS": "FALSE"}, "Ydb", "host"))
	require.Equal(t, "/owner/tmp", d.Get("initial_token_secret_path"))
	require.Equal(t, "cloud", d.Get("resource_id"))
	require.Equal(t, "CALLER_TOKEN", d.Get("iam_bootstrap_mode"))
	require.NoError(t, flattenDescription(d, entity, map[string]string{}, "Ydb", "host"))
	require.Equal(t, "", d.Get("initial_token_secret_path"))
	require.Equal(t, "", d.Get("resource_id"))
}
