package topic

import (
	"fmt"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

// validateOptionalRetentionPeriod allows unset values and otherwise checks Go duration syntax via time.ParseDuration.
// terraform-plugin-sdk helper/validation has no duration validator; StringIsEmpty + StringIsNotEmpty cover optional/required string shape.
var validateOptionalRetentionPeriod schema.SchemaValidateDiagFunc = validation.ToDiagFunc(
	validation.Any(
		validation.StringIsEmpty,
		validation.All(
			validation.StringIsNotEmpty,
			positiveGoDuration,
		),
	),
)

func positiveGoDuration(i interface{}, k string) ([]string, []error) {
	v, ok := i.(string)
	if !ok {
		return nil, []error{fmt.Errorf("expected type of %q to be string", k)}
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return nil, []error{fmt.Errorf("%q must be a valid Go duration (e.g. \"24h\", \"30m\"): %w", k, err)}
	}
	if d <= 0 {
		return nil, []error{fmt.Errorf("%q must be greater than zero", k)}
	}
	return nil, nil
}

func retentionPeriodDiffSuppress(_, old, newVal string, _ *schema.ResourceData) bool {
	if old == newVal {
		return true
	}
	oldD, err1 := time.ParseDuration(old)
	newD, err2 := time.ParseDuration(newVal)
	if err1 != nil || err2 != nil {
		return false
	}
	return oldD == newD
}

func retentionPeriodFromResourceData(d *schema.ResourceData) (time.Duration, bool, error) {
	for _, name := range []string{attributeRetentionPeriod, attributeRetentionPeriodHours} {
		period, ok, err := parseRetentionPeriodField(name, d.Get(name))
		if err != nil {
			return 0, false, err
		}
		if ok {
			return period, true, nil
		}
	}
	return 0, false, nil
}

func retentionPeriodFieldValue(d *schema.ResourceData, name string, old bool) (time.Duration, bool, error) {
	var raw interface{}
	if d.HasChange(name) {
		oldVal, newVal := d.GetChange(name)
		if old {
			raw = oldVal
		} else {
			raw = newVal
		}
	} else {
		raw = d.Get(name)
	}
	return parseRetentionPeriodField(name, raw)
}

func parseRetentionPeriodField(name string, raw interface{}) (time.Duration, bool, error) {
	switch name {
	case attributeRetentionPeriod:
		s, ok := raw.(string)
		if !ok || s == "" {
			return 0, false, nil
		}
		period, err := time.ParseDuration(s)
		if err != nil {
			return 0, true, fmt.Errorf("failed to parse %q: %w", attributeRetentionPeriod, err)
		}
		return period, true, nil
	case attributeRetentionPeriodHours:
		i, ok := raw.(int)
		if !ok || i == 0 {
			return 0, false, nil
		}
		return time.Duration(i) * time.Hour, true, nil
	default:
		return 0, false, nil
	}
}

func retentionPeriodAtSide(d *schema.ResourceData, old bool) (time.Duration, bool, error) {
	for _, name := range []string{attributeRetentionPeriod, attributeRetentionPeriodHours} {
		period, ok, err := retentionPeriodFieldValue(d, name, old)
		if err != nil {
			return 0, false, err
		}
		if ok {
			return period, true, nil
		}
	}
	return 0, false, nil
}

func retentionPeriodNeedsAlter(d *schema.ResourceData) (time.Duration, bool, error) {
	if !hasRetentionPeriodChange(d) {
		return 0, false, nil
	}
	oldPeriod, oldOk, err := retentionPeriodAtSide(d, true)
	if err != nil {
		return 0, false, err
	}
	newPeriod, newOk, err := retentionPeriodAtSide(d, false)
	if err != nil {
		return 0, false, err
	}
	if oldOk && newOk && oldPeriod == newPeriod {
		return 0, false, nil
	}
	if newOk {
		return newPeriod, true, nil
	}
	return 0, false, nil
}

func hasRetentionPeriodChange(d *schema.ResourceData) bool {
	return d.HasChange(attributeRetentionPeriod) || d.HasChange(attributeRetentionPeriodHours)
}
