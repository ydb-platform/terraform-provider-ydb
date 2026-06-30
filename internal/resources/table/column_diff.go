package table

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// ValidateResourceDiffColumns rejects unsupported column schema changes at plan time.
func ValidateResourceDiffColumns(d *schema.ResourceDiff) error {
	if !d.HasChange("column") && len(d.GetChangedKeysPrefix("column")) == 0 {
		return nil
	}
	o, n := d.GetChange("column")
	if o == nil || n == nil {
		return nil
	}
	_, err := checkColumnDiff(expandColumns(n), expandColumns(o))
	return err
}
