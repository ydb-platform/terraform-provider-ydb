package externaldatasource

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// ValidateResourceDiffAuth validates auth fields from the planned diff (CustomizeDiff / plan).
func ValidateResourceDiffAuth(d *schema.ResourceDiff) error {
	return validateResourceAuth(resourceFromDiff(d), d)
}

// ValidateResourceDiffSourceType validates auth_method and properties against source_type.
func ValidateResourceDiffSourceType(d *schema.ResourceDiff) error {
	return validateSourceType(resourceFromDiff(d))
}

func resourceFromDiff(d *schema.ResourceDiff) *Resource {
	vals := make(map[string]string, len(allStringAttrKeys))
	for _, k := range allStringAttrKeys {
		vals[k] = diffString(d, k)
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
