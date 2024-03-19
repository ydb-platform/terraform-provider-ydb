package kv

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/assert"
)

func TestStorageCfgParse(t *testing.T) {
	testSchema := map[string]*schema.Schema{
		"storage_config": {
			Type: schema.TypeList,
			Elem: &schema.Resource{
				Schema: map[string]*schema.Schema{
					"channel": {
						Type: schema.TypeList,
						Elem: &schema.Resource{
							Schema: map[string]*schema.Schema{
								"media": {
									Type: schema.TypeString,
								},
							},
						},
					},
				},
			},
		},
	}

	testData := map[string]interface{}{
		"storage_config": []interface{}{
			map[string]interface{}{
				"channel": []interface{}{
					map[string]interface{}{
						"media": "ssd",
					},
					map[string]interface{}{
						"media": "ssd",
					},
					map[string]interface{}{
						"media": "ssd",
					},
				},
			},
		},
	}
	// expected result
	expectedMedias := []string{"ssd", "ssd", "ssd"}

	d := schema.TestResourceDataRaw(t, testSchema, testData)
	chancfg, err := expandStorageConfig(d)
	if err != nil {
		t.Error(err)
	}
	// test result
	foundMedia := make([]string, len(chancfg.Channel))
	for i, v := range chancfg.Channel {
		foundMedia[i] = v.Media
	}
	assert.Equal(t, expectedMedias, foundMedia)
}
