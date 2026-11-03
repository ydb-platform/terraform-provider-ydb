package externaldatasource

import (
	"context"
	"net/url"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

type mutationKind uint8

const (
	createMutation mutationKind = iota
	replaceMutation
)

func (h *Handler) mutate(ctx context.Context, d *schema.ResourceData, kind mutationKind) diag.Diagnostics {
	r, err := resourceSchemaToResource(d)
	if err != nil {
		return diag.FromErr(err)
	}
	if err := validateIAMConfig(r, d.GetRawConfig()); err != nil {
		return diag.FromErr(err)
	}
	if err := validateResourceAuth(r, nil); err != nil {
		return diag.FromErr(err)
	}
	if err := validateSourceType(r, nil); err != nil {
		return diag.FromErr(err)
	}
	if kind == replaceMutation && r.Entity == nil {
		return diag.Errorf("external data source id is required for update")
	}
	endpoint := r.getConnectionString()
	if diags := guardIAMMutation(d); diags.HasError() {
		return diags
	}
	databaseURL, err := url.Parse(endpoint)
	if err != nil {
		return diag.Errorf("invalid database endpoint")
	}
	fullPath := databaseURL.Query().Get("database") + "/" + r.Path
	if r.Entity != nil {
		fullPath = r.Entity.GetFullEntityPath()
	}
	b, err := h.open(ctx, endpoint)
	if err != nil {
		return diag.Errorf("failed to initialize client: %s", safeStatus(err))
	}
	defer h.close(ctx, b)
	iam := r.strAttr("auth_method") == "IAM"
	if iam {
		_ = d.Set("iam_bootstrap_mode", r.strAttr("iam_bootstrap_mode"))
		if !Configured(d.GetRawConfig(), "resource_id") && (d.HasChange("database_id") || d.HasChange("database_name") || d.HasChange("location")) {
			r.Values["resource_id"] = ""
		}
	}
	result := mutateResource(ctx, b, r, fullPath, kind)
	if result.outcome != terminalSuccess {
		return mutationDiagnostics(d, kind, iam, result)
	}
	if kind == createMutation {
		d.SetId(endpoint + "?path=" + r.Path)
	}
	var diags diag.Diagnostics
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return append(diags, diag.FromErr(err)...)
	}
	desc, err := b.Describe(ctx, entity.GetFullEntityPath())
	if err != nil {
		if !iam {
			return diag.Errorf("failed to describe external data source: %s", safeStatus(err))
		}
		// An acknowledged Create must not be tainted solely because read-back failed.
		return append(diags, diag.Diagnostic{Severity: diag.Warning, Summary: "EDS mutation acknowledged; refresh required", Detail: "Describe failed: " + safeStatus(err) + ". The confirmed EDS identity is retained; retry refresh."})
	}
	return append(diags, diag.FromErr(flattenDescription(d, entity, desc.Properties, desc.SourceType, desc.Location))...)
}

func mutateResource(ctx context.Context, b backend, r *Resource, fullPath string, kind mutationKind) mutationResult {
	q := PrepareDataSourceQuery(fullPath, r)
	if kind == createMutation {
		q = PrepareCreateDataSourceQuery(fullPath, r)
	}
	return execMutation(ctx, b, q)
}

func mutationDiagnostics(d *schema.ResourceData, kind mutationKind, iam bool, result mutationResult) diag.Diagnostics {
	if iam && result.outcome == unknownOutcome {
		if kind == replaceMutation {
			_ = d.Set("pending_iam_mutation", "UNKNOWN")
		}
		return diag.Errorf("EDS mutation outcome is unknown: %s. Reconcile server-side execution; an equal Describe does not establish Create ownership. Import only after verifying ownership", safeStatus(result.err))
	}
	return diag.Errorf("failed to mutate external data source: %s", safeStatus(result.err))
}
