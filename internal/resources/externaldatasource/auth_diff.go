package externaldatasource

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// ValidateResourceDiffAuth validates auth fields from the planned diff (CustomizeDiff / plan).
func ValidateResourceDiffAuth(d *schema.ResourceDiff) error {
	r := resourceFromDiff(d)
	if err := validateIAMConfig(r, d.GetRawConfig(), d); err != nil {
		return err
	}
	return validateResourceAuth(r, d)
}

// ValidateResourceDiffSourceType validates auth_method and properties against source_type.
func ValidateResourceDiffSourceType(d *schema.ResourceDiff) error {
	return validateSourceType(resourceFromDiff(d), d)
}

func resourceFromDiff(d *schema.ResourceDiff) *Resource {
	vals := make(map[string]string, len(allStringAttrKeys))
	for _, k := range allStringAttrKeys {
		vals[k] = diffString(d, k)
	}
	vals["iam_bootstrap_mode"] = diffString(d, "iam_bootstrap_mode")
	r := &Resource{Values: vals}
	if v, ok := d.GetOkExists("use_tls"); ok && (d.GetRawConfig().IsNull() || Configured(d.GetRawConfig(), "use_tls")) {
		if b, ok := v.(bool); ok {
			r.UseTLS = &b
		}
	}
	return r
}

func diffString(d *schema.ResourceDiff, key string) string {
	v := d.Get(key)
	if v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// CustomizeDiff validates IAM configuration and invalidates an implicitly resolved
// target cloud ID when the target changes. Explicit resource IDs remain authoritative.
func CustomizeDiff(d *schema.ResourceDiff) error {
	if d.NewValueKnown("auth_method") && diffString(d, "auth_method") != "IAM" {
		for _, key := range []string{"iam_bootstrap_mode", "initial_token_secret_path", "resource_id"} {
			if !Configured(d.GetRawConfig(), key) {
				if err := d.SetNew(key, ""); err != nil {
					return err
				}
			}
		}
	}
	if err := ValidateResourceDiffAuth(d); err != nil {
		return err
	}
	if err := ValidateResourceDiffSourceType(d); err != nil {
		return err
	}
	if diffString(d, "auth_method") == "IAM" {
		if diffString(d, "iam_bootstrap_mode") == "" && d.NewValueKnown("iam_bootstrap_mode") {
			if err := d.SetNew("iam_bootstrap_mode", "CALLER_TOKEN"); err != nil {
				return err
			}
		}
		if !Configured(d.GetRawConfig(), "resource_id") && (d.HasChange("database_id") || d.HasChange("database_name") || d.HasChange("location")) {
			return d.SetNewComputed("resource_id")
		}
	}
	return nil
}
