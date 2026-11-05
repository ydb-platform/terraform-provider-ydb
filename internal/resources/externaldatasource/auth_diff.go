package externaldatasource

import (
	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// CustomizeDiff preserves explicit cloud IDs and invalidates observed IDs when the target changes.
func CustomizeDiff(d *schema.ResourceDiff) error {
	if d.NewValueKnown("auth_method") && diffString(d, "auth_method") != "IAM" && !resourceIDConfigured(d.GetRawConfig()) {
		if err := d.SetNew("resource_id", ""); err != nil {
			return err
		}
	}
	if err := ValidateResourceDiffAuth(d); err != nil {
		return err
	}
	if err := ValidateResourceDiffSourceType(d); err != nil {
		return err
	}
	if diffString(d, "auth_method") == "IAM" && !resourceIDConfigured(d.GetRawConfig()) &&
		(d.HasChange("database_name") || d.HasChange("database_id") || d.HasChange("location")) {
		return d.SetNewComputed("resource_id")
	}
	return nil
}

func resourceIDConfigured(raw cty.Value) bool {
	// Direct SDK callers may omit raw config; preserve their supplied cloud ID.
	if raw.IsNull() || !raw.IsKnown() {
		return true
	}
	return raw.Type().IsObjectType() && raw.Type().HasAttribute("resource_id") && !raw.GetAttr("resource_id").IsNull()
}

// ValidateResourceDiffAuth validates auth fields from the planned diff (CustomizeDiff / plan).
func ValidateResourceDiffAuth(d *schema.ResourceDiff) error {
	return validateResourceAuth(resourceFromDiff(d), d)
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
	if !resourceIDConfigured(d.GetRawConfig()) && (d.HasChange("database_name") || d.HasChange("database_id") || d.HasChange("location")) {
		vals["resource_id"] = ""
	}
	r := &Resource{Values: vals}
	if v, ok := d.GetOk("use_tls"); ok {
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
