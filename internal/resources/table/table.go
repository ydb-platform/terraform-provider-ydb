package table

import (
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
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
			"indexes": {
				Type:     schema.TypeList,
				Optional: true,
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
				Required: true,
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
