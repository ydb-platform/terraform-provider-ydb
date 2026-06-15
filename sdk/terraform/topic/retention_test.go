package topic

import (
	"testing"
	"time"

	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func TestValidateOptionalRetentionPeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "valid hours", value: "24h"},
		{name: "valid minutes", value: "30m"},
		{name: "valid composite", value: "1h30m"},
		{name: "empty allowed", value: ""},
		{name: "invalid format", value: "not-a-duration", wantErr: true},
		{name: "zero duration", value: "0s", wantErr: true},
		{name: "negative duration", value: "-1h", wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			diags := validateOptionalRetentionPeriod(tt.value, cty.Path{cty.GetAttrStep{Name: attributeRetentionPeriod}})
			if diags.HasError() != tt.wantErr {
				t.Fatalf("validateOptionalRetentionPeriod(%q) diags = %v, wantErr %v", tt.value, diags, tt.wantErr)
			}
		})
	}
}

func TestRetentionPeriodFromResourceData(t *testing.T) {
	t.Parallel()

	t.Run("from retention_period", func(t *testing.T) {
		t.Parallel()
		d := schema.TestResourceDataRaw(t, map[string]*schema.Schema{
			attributeRetentionPeriod: {Type: schema.TypeString, Optional: true},
		}, map[string]interface{}{
			attributeRetentionPeriod: "48h",
		})

		period, ok, err := retentionPeriodFromResourceData(d)
		if err != nil {
			t.Fatalf("retentionPeriodFromResourceData: %v", err)
		}
		if !ok {
			t.Fatal("expected ok=true")
		}
		if period != 48*time.Hour {
			t.Fatalf("period = %v, want %v", period, 48*time.Hour)
		}
	})

	t.Run("from retention_period_hours", func(t *testing.T) {
		t.Parallel()
		d := schema.TestResourceDataRaw(t, map[string]*schema.Schema{
			attributeRetentionPeriodHours: {Type: schema.TypeInt, Optional: true},
		}, map[string]interface{}{
			attributeRetentionPeriodHours: 24,
		})

		period, ok, err := retentionPeriodFromResourceData(d)
		if err != nil {
			t.Fatalf("retentionPeriodFromResourceData: %v", err)
		}
		if !ok {
			t.Fatal("expected ok=true")
		}
		if period != 24*time.Hour {
			t.Fatalf("period = %v, want %v", period, 24*time.Hour)
		}
	})

	t.Run("not set", func(t *testing.T) {
		t.Parallel()
		d := schema.TestResourceDataRaw(t, map[string]*schema.Schema{
			attributeRetentionPeriod: {Type: schema.TypeString, Optional: true},
		}, map[string]interface{}{})

		_, ok, err := retentionPeriodFromResourceData(d)
		if err != nil {
			t.Fatalf("retentionPeriodFromResourceData: %v", err)
		}
		if ok {
			t.Fatal("expected ok=false")
		}
	})
}

func TestRetentionPeriodNeedsAlter(t *testing.T) {
	t.Parallel()

	d := schema.TestResourceDataRaw(t, map[string]*schema.Schema{
		attributeRetentionPeriod:      {Type: schema.TypeString, Optional: true, Computed: true},
		attributeRetentionPeriodHours: {Type: schema.TypeInt, Optional: true, Computed: true},
	}, map[string]interface{}{
		attributeRetentionPeriodHours: 13,
	})

	d.SetId("test")
	if err := d.Set(attributeRetentionPeriod, "13h"); err != nil {
		t.Fatalf("Set retention_period: %v", err)
	}

	period, alter, err := retentionPeriodNeedsAlter(d)
	if err != nil {
		t.Fatalf("retentionPeriodNeedsAlter: %v", err)
	}
	if alter {
		t.Fatalf("expected no alter for equivalent 13h and 13 hours, got period=%v", period)
	}
}

func TestRetentionPeriodDiffSuppress(t *testing.T) {
	t.Parallel()

	if !retentionPeriodDiffSuppress("", "13h0m0s", "13h", nil) {
		t.Fatal("expected equivalent durations to suppress diff")
	}
	if retentionPeriodDiffSuppress("", "13h", "14h", nil) {
		t.Fatal("expected different durations not to suppress diff")
	}
}

func TestParseRetentionPeriodField_equivalentDurations(t *testing.T) {
	t.Parallel()

	p1, ok1, err := parseRetentionPeriodField(attributeRetentionPeriod, "13h")
	if err != nil || !ok1 {
		t.Fatalf("parse 13h: ok=%v err=%v", ok1, err)
	}
	p2, ok2, err := parseRetentionPeriodField(attributeRetentionPeriod, "13h0m0s")
	if err != nil || !ok2 {
		t.Fatalf("parse 13h0m0s: ok=%v err=%v", ok2, err)
	}
	if p1 != p2 {
		t.Fatalf("13h and 13h0m0s differ: %v vs %v", p1, p2)
	}

	p3, ok3, err := parseRetentionPeriodField(attributeRetentionPeriodHours, 13)
	if err != nil || !ok3 {
		t.Fatalf("parse 13 hours: ok=%v err=%v", ok3, err)
	}
	if p1 != p3 {
		t.Fatalf("13h and 13 hours differ: %v vs %v", p1, p3)
	}
}
