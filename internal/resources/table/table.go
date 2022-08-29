package table

import (
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func ResourceYDBTable() *schema.Resource {
	return &schema.Resource{
		CreateContext: TableCreate,
		ReadContext:   TableRead,
		UpdateContext: TableUpdate,
		DeleteContext: TableDelete,
		Schema: map[string]*schema.Schema{
			"path": {
				Type:     schema.TypeString,
				Required: true,
			},
			"database_endpoint": {
				Type:     schema.TypeString,
				Required: true,
			},
			"column": {
				Type:     schema.TypeList,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.NoZeroValues,
						},
						"type": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.NoZeroValues,
						},
						"family": {
							Type:     schema.TypeString,
							Optional: true,
							Computed: true, // TODO(shmel1k@): ?
						},
					},
				},
			},
			"primary_key": {
				Type:     schema.TypeList,
				Required: true,
				Elem: &schema.Schema{
					Type:         schema.TypeString,
					ValidateFunc: nil, // TODO(shmel1k@): think about validate func
				},
			},
			"index": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.NoZeroValues,
						},
						// "type": {
						// 	Type:         schema.TypeString,
						// 	Required:     true,
						// 	ValidateFunc: validation.NoZeroValues,
						// },
						"columns": {
							Type:     schema.TypeList,
							Required: true,
							Elem: &schema.Schema{
								Type:         schema.TypeString,
								ValidateFunc: validation.NoZeroValues,
							},
						},
					},
				},
			},
			"ttl": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"column_name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.NoZeroValues,
						},
						"mode": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice([]string{"date_type", "since_unix_epoch"}, false),
						},
						"expire_after_seconds": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.NoZeroValues,
						},
					},
				},
			},
			"attribute": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"key": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.NoZeroValues,
						},
						"value": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.NoZeroValues,
						},
					},
				},
			},
			// TODO(shmel1k@): should we add more keys?
		},
	}
}

type TableColumn struct {
	Name   string
	Type   string
	Family string
}

type TablePrimaryKey struct {
	Columns []string
}

type TableIndex struct {
	Name    string
	Type    string
	Columns []string
}

type TableTTL struct {
	ColumnName  string
	Mode        string
	ExpireAfter time.Duration
}

type TableResource struct {
	Path             string
	DatabaseEndpoint string
	Columns          []*TableColumn
	PrimaryKey       *TablePrimaryKey
	Indexes          []*TableIndex
}

func tableResourceSchemaToTableResource(d *schema.ResourceData) *TableResource {
	columnsRaw := d.Get("column").([]interface{})
	columns := make([]*TableColumn, 0, len(columnsRaw))
	for _, v := range columnsRaw {
		mp := v.(map[string]interface{})
		family := ""
		if f, ok := mp["family"].(string); ok {
			family = f
		}
		col := &TableColumn{
			Name:   mp["name"].(string),
			Type:   mp["type"].(string),
			Family: family,
		}
		columns = append(columns, col)
	}

	pkRaw := d.Get("primary_key").([]interface{})
	pk := make([]string, 0, len(pkRaw))
	for _, v := range pkRaw {
		pk = append(pk, v.(string))
	}

	indexesRaw := d.Get("indexes")
	var indexes []*TableIndex
	if indexesRaw != nil {
		raw := indexesRaw.([]map[string]interface{})
		for _, r := range raw {
			name := r["name"].(string)
			typ := r["type"].(string)
			colsRaw := r["columns"].([]interface{})
			colsArr := make([]string, 0, len(colsRaw))
			for _, c := range colsRaw {
				colsArr = append(colsArr, c.(string))
			}
			indexes = append(indexes, &TableIndex{
				Name:    name,
				Type:    typ,
				Columns: colsArr,
			})
		}
	}

	return &TableResource{
		Path:             d.Get("path").(string),
		DatabaseEndpoint: d.Get("database_endpoint").(string),
		Columns:          columns,
		Indexes:          indexes,
		PrimaryKey: &TablePrimaryKey{
			Columns: pk,
		},
	}
}

func flattenTableDescription(d *schema.ResourceData, desc options.Description) {
	_ = d.Set("path", desc.Name) // TODO(shmel1k@): path?

	cols := make([]interface{}, 0, len(desc.Columns))
	for _, col := range desc.Columns {
		mp := make(map[string]interface{})
		mp["name"] = col.Name
		mp["type"] = col.Type.String()
		mp["family"] = col.Family
		cols = append(cols, mp)
	}
	_ = d.Set("columns", cols)

	pk := make([]interface{}, 0, len(desc.PrimaryKey))
	for _, p := range desc.PrimaryKey {
		pk = append(pk, p)
	}
	_ = d.Set("primary_key", pk)

	indexes := make([]interface{}, 0, len(desc.Indexes))
	for _, idx := range desc.Indexes {
		mp := make(map[string]interface{})
		mp["name"] = idx.Name
		// TODO(shmel1k@): index type?
		cols := make([]interface{}, 0, len(idx.IndexColumns))
		for _, c := range idx.IndexColumns {
			cols = append(cols, c)
		}
		mp["columns"] = cols
	}
	_ = d.Set("index", indexes)

	if desc.TimeToLiveSettings != nil {
		ttlSettings := make(map[string]interface{})
		ttlSettings["column_name"] = desc.TimeToLiveSettings.ColumnName
		ttlSettings["mode"] = desc.TimeToLiveSettings.Mode
		ttlSettings["expire_after_seconds"] = desc.TimeToLiveSettings.ExpireAfterSeconds
		_ = d.Set("ttl", ttlSettings)
	}

	attributes := make([]interface{}, 0, len(desc.Attributes))
	for k, v := range desc.Attributes {
		attributes = append(attributes, map[string]string{
			"key":   k,
			"value": v,
		})
	}
	_ = d.Set("attribute", attributes)
}
