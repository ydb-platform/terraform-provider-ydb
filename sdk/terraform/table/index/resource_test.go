package index

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestIndexTypePlanAfterRead(t *testing.T) {
	res := &schema.Resource{Schema: map[string]*schema.Schema{"type": ResourceSchema()["type"]}}

	for _, tc := range []struct {
		name            string
		config          map[string]interface{}
		wantReplacement bool
	}{
		{name: "omitted type", config: map[string]interface{}{}},
		{name: "empty type", config: map[string]interface{}{"type": ""}},
		{name: "explicit sync", config: map[string]interface{}{"type": "global_sync"}},
		{name: "async change", config: map[string]interface{}{"type": "global_async"}, wantReplacement: true},
		{name: "unique change", config: map[string]interface{}{"type": "global_unique_index"}, wantReplacement: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &terraform.InstanceState{
				ID:         "index-id",
				Attributes: map[string]string{"type": "global_sync"},
			}
			diff, err := res.Diff(context.Background(), state, terraform.NewResourceConfigRaw(tc.config), nil)
			if err != nil {
				t.Fatalf("plan index type: %v", err)
			}
			if !tc.wantReplacement {
				if diff != nil && len(diff.Attributes) != 0 {
					t.Fatalf("unexpected plan after read: %+v", diff.Attributes)
				}
				return
			}
			if diff == nil || diff.Attributes["type"] == nil || !diff.Attributes["type"].RequiresNew {
				t.Fatalf("type change should replace index, got diff: %+v", diff)
			}
		})
	}
}
