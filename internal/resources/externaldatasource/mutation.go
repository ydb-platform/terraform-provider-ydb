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
	if err := validateIAMConfig(r, d.GetRawConfig(), nil); err != nil {
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
	if diags := h.recoverCleanup(ctx, d, endpoint); diags.HasError() {
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
	mutate := func(p string) mutationResult {
		return mutateResource(ctx, b, r, fullPath, p, kind)
	}
	var result bootstrapResult
	if iam && r.strAttr("iam_bootstrap_mode") != "EXISTING_SECRET" {
		result = h.bootstrap(ctx, endpoint, databaseURL.Query().Get("database"), b, mutate)
	} else {
		result = bootstrapResult{mutationResult: mutate(""), stage: edsStage}
	}
	if result.outcome != terminalSuccess {
		return mutationDiagnostics(d, kind, iam, result)
	}
	if kind == createMutation {
		d.SetId(endpoint + "?path=" + r.Path)
	}
	var diags diag.Diagnostics
	if result.cleanupErr != nil {
		_ = d.Set("pending_cleanup_path", result.path)
		diags = append(diags, diag.Diagnostic{Severity: diag.Warning, Summary: "EDS saved; temporary IAM secret cleanup pending", Detail: "Confirmed-owned secret " + result.path + " could not be removed (" + safeStatus(result.cleanupErr) + "). EDS identity is retained without taint; refresh retries cleanup. Do not remove pending_cleanup_path from state."})
	}
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

func mutateResource(ctx context.Context, b backend, r *Resource, fullPath, secretPath string, kind mutationKind) mutationResult {
	vals := make(map[string]string, len(r.Values))
	for k, v := range r.Values {
		vals[k] = v
	}
	transient := *r
	transient.Values = vals
	if secretPath != "" {
		transient.Values["initial_token_secret_path"] = secretPath
	}
	q := PrepareDataSourceQuery(fullPath, &transient)
	if kind == createMutation {
		q = PrepareCreateDataSourceQuery(fullPath, &transient)
	}
	return execMutation(ctx, b, q)
}

func mutationDiagnostics(d *schema.ResourceData, kind mutationKind, iam bool, result bootstrapResult) diag.Diagnostics {
	if !iam {
		return diag.Errorf("failed to mutate external data source: %s", safeStatus(result.err))
	}
	if kind == replaceMutation {
		if result.owned && (result.cleanupErr != nil || result.outcome == unknownOutcome) {
			_ = d.Set("pending_cleanup_path", result.path)
		}
		if result.outcome == unknownOutcome && result.stage == edsStage {
			_ = d.Set("pending_iam_mutation", "UNKNOWN")
		}
	}
	return bootstrapDiagnostics(result)
}
