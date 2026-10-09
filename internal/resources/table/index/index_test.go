package index

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func TestGetIndexType(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typeID options.IndexType
		want   string
	}{
		{name: "sync", typeID: options.IndexTypeGlobal, want: "global_sync"},
		{name: "async", typeID: options.IndexTypeGlobalAsync, want: "global_async"},
		{name: "unique", typeID: options.IndexTypeGlobalUnique, want: "global_unique_index"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := getIndexType(tc.typeID); got != tc.want {
				t.Errorf("getIndexType(%v) = %q, want %q", tc.typeID, got, tc.want)
			}
		})
	}
}

func TestFlattenIndexDescriptionReportsActualType(t *testing.T) {
	d := schema.TestResourceDataRaw(t, map[string]*schema.Schema{
		"table_path":        {Type: schema.TypeString, Optional: true},
		"connection_string": {Type: schema.TypeString, Optional: true},
		"table_id":          {Type: schema.TypeString, Optional: true},
		"name":              {Type: schema.TypeString, Optional: true},
		"type":              {Type: schema.TypeString, Optional: true},
		"columns":           {Type: schema.TypeList, Optional: true, Elem: &schema.Schema{Type: schema.TypeString}},
		"cover":             {Type: schema.TypeList, Optional: true, Elem: &schema.Schema{Type: schema.TypeString}},
	}, nil)

	indexResource := &resource{
		TablePath:        "table",
		ConnectionString: "grpc://localhost:2136/?database=/local",
	}
	desc := options.IndexDescription{
		Name:         "email_idx",
		Type:         options.IndexTypeGlobal,
		IndexColumns: []string{"email"},
	}
	if err := flattenIndexDescription(d, indexResource, desc); err != nil {
		t.Fatalf("flattenIndexDescription: %v", err)
	}
	if got := d.Get("type"); got != TypeGlobalSync {
		t.Errorf("index type in state = %q, want %q", got, TypeGlobalSync)
	}
}
