package table

import (
	"fmt"
	"net/url"

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
					},
				},
			},
			"primary_key": {
				Type:     schema.TypeList,
				Required: true,
				Elem: &schema.Schema{
					Type:         schema.TypeString,
					ValidateFunc: validation.NoZeroValues, // TODO(shmel1k@): think about validate func
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
						"type": {
							Type:         schema.TypeString,
							Optional:     true,
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
						"column_unit": {
							Type:     schema.TypeString,
							Optional: true,
							ValidateFunc: validation.StringInSlice([]string{
								"ns",
								"us",
								"ms",
								"s",
							}, false),
						},
					},
				},
			},
			"attributes": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"auto_partitioning": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"by_size": {
							Type:         schema.TypeInt,
							Optional:     true,
							Computed:     true,
							ValidateFunc: validation.NoZeroValues,
							// TODO(shmel1k@): check default values.
						},
						"by_load": {
							Type:     schema.TypeBool,
							Optional: true,
							Computed: true,
						},
					},
				},
			},
			"partitioning_policy": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"type": {
							Type:     schema.TypeString,
							Optional: true,
							Computed: true,
						},
						"partitions_count": {
							Type:     schema.TypeInt,
							Optional: true,
						},
						"explicit_partitions": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeInt,
							},
						},
						"min_partitions_count": {
							Type:     schema.TypeInt,
							Optional: true,
						},
						"max_partitions_count": {
							Type:     schema.TypeInt,
							Optional: true,
						},
					},
				},
			},

			"primary_key_bloom_filter": {
				Type:     schema.TypeBool,
				Optional: true,
				Computed: true,
			},
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
	Cover   []string
}

type TableTTL struct {
	ColumnName string
	Interval   string
}

type TablePartitioningSettings struct {
	BySize             *int
	ByLoad             *bool
	PartitionAtKeys    []int
	PartitionsCount    int
	MinPartitionsCount int
	MaxPartitionsCount int
}

type TableReplicationSettings struct {
	ReadReplicasSettings string
}

type TableFamily struct {
	Name        string
	Data        string
	Compression string
}

type TableChangeDataCaptureSettings struct {
	Mode   string
	Format string
}

type TableResource struct {
	Path                 string
	DatabaseEndpoint     string
	Token                string
	Attributes           map[string]string
	Family               []*TableFamily
	Columns              []*TableColumn
	Indexes              []*TableIndex
	PrimaryKey           *TablePrimaryKey
	TTL                  *TableTTL
	ReplicationSettings  *TableReplicationSettings
	PartitioningSettings *TablePartitioningSettings
	EnableBloomFilter    *bool
}

func expandTableTTLSettings(d *schema.ResourceData) (ttl *TableTTL) {
	v, ok := d.GetOk("ttl")
	if !ok {
		return
	}
	ttlSet := v.(*schema.Set)
	for _, l := range ttlSet.List() {
		m := l.(map[string]interface{})
		ttl = &TableTTL{}
		ttl.Interval = m["interval"].(string)
		ttl.ColumnName = m["column_name"].(string)
	}
	return
}

func expandTableReplicasSettings(d *schema.ResourceData) (p *TableReplicationSettings) {
	v, ok := d.GetOk("read_replicas_settings")
	if !ok {
		return
	}

	p = &TableReplicationSettings{}
	p.ReadReplicasSettings = v.(string)
	return
}

func expandTablePartitioningPolicySettings(d *schema.ResourceData) (p *TablePartitioningSettings) {
	v, ok := d.GetOk("partitioning_policy")
	if !ok {
		return
	}

	p = &TablePartitioningSettings{}

	pSet := v.(*schema.Set)
	for _, l := range pSet.List() {
		m := l.(map[string]interface{})
		if partitionsCount, ok := m["partitions_count"].(int); ok {
			p.PartitionsCount = partitionsCount
		}
		if explicitPartitions, ok := m["explicit_partitions"].([]interface{}); ok {
			for _, v := range explicitPartitions {
				p.PartitionAtKeys = append(p.PartitionAtKeys, v.(int))
			}
		}
		if minPartitionsCount, ok := m["min_partitions_count"].(int); ok {
			p.MinPartitionsCount = minPartitionsCount
		}
		if maxPartitionsCount, ok := m["max_partitions_count"].(int); ok {
			p.MaxPartitionsCount = maxPartitionsCount
		}
		if byLoad, ok := m["by_load"].(bool); ok {
			p.ByLoad = &byLoad
		}
		if bySize, ok := m["by_size"].(int); ok {
			p.BySize = &bySize
		}
	}

	return
}

func tableResourceSchemaToTableResource(d *schema.ResourceData) (*TableResource, error) {
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

	indexesRaw := d.Get("index")
	var indexes []*TableIndex
	if indexesRaw != nil {
		raw := indexesRaw.([]interface{})
		for _, rw := range raw {
			r := rw.(map[string]interface{})
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

	attributesRaw := d.Get("attributes")
	attributes := make(map[string]string)
	// TODO(shmel1k@): add sorting.
	if attributesRaw != nil {
		raw := attributesRaw.(map[string]interface{})
		for k, v := range raw {
			attributes[k] = v.(string)
		}
	}

	ttl := expandTableTTLSettings(d)

	token := ""
	if tok, ok := d.GetOk("token"); ok {
		token = tok.(string)
	}

	databaseEndpoint := d.Get("database_endpoint").(string)
	databaseURL, err := url.Parse(databaseEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database endpoint: %s", err)
	}

	partitioningSettings := expandTablePartitioningPolicySettings(d)
	replicasSettings := expandTableReplicasSettings(d)

	var bloomFilterEnabled *bool
	if v, ok := d.GetOk("primary_key_bloom_filter"); ok {
		b := v.(bool)
		bloomFilterEnabled = &b
	}

	return &TableResource{
		Path:             databaseURL.Query().Get("database") + "/" + d.Get("path").(string),
		DatabaseEndpoint: d.Get("database_endpoint").(string),
		Attributes:       attributes,
		Columns:          columns,
		Indexes:          indexes,
		PrimaryKey: &TablePrimaryKey{
			Columns: pk,
		},
		TTL:                  ttl,
		PartitioningSettings: partitioningSettings,
		ReplicationSettings:  replicasSettings,
		Token:                token,
		EnableBloomFilter:    bloomFilterEnabled,
	}, nil
}

func flattenTableDescription(d *schema.ResourceData, desc options.Description, database string) {
	_ = d.Set("path", desc.Name) // TODO(shmel1k@): path?

	cols := make([]interface{}, 0, len(desc.Columns))
	for _, col := range desc.Columns {
		mp := make(map[string]interface{})
		mp["name"] = col.Name
		mp["type"] = col.Type.String() // TODO(shmel1k@): why optional?
		// mp["family"] = col.Family
		cols = append(cols, mp)
	}
	_ = d.Set("column", cols)

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
		indexes = append(indexes, mp)
	}
	_ = d.Set("index", indexes)

	if desc.TimeToLiveSettings != nil {
		var ttlSettings []interface{}
		ttlSettings = append(ttlSettings, map[string]interface{}{
			"column_name":          desc.TimeToLiveSettings.ColumnName,
			"mode":                 desc.TimeToLiveSettings.Mode,
			"expire_after_seconds": desc.TimeToLiveSettings.ExpireAfterSeconds,
			"column_unit":          desc.TimeToLiveSettings.ColumnUnit.ToYDB().String(),
		})
		_ = d.Set("ttl", ttlSettings)
	}

	attributes := make(map[string]interface{})
	for k, v := range desc.Attributes {
		attributes[k] = v
	}
	_ = d.Set("attributes", attributes)

	var autoPartitioningSettings []interface{}
	autoPartitioningSettings = append(autoPartitioningSettings, map[string]interface{}{
		"by_load": desc.PartitioningSettings.PartitioningByLoad == options.FeatureEnabled,
		"by_size": desc.PartitioningSettings.PartitioningBySize == options.FeatureEnabled,
	})
	_ = d.Set("auto_partitioning", autoPartitioningSettings)

	var partitioningPolicy []interface{}
	pol := map[string]interface{}{
		"max_partitions_count": desc.PartitioningSettings.MaxPartitionsCount,
		"min_partitions_count": desc.PartitioningSettings.MinPartitionsCount,
	}
	if desc.Stats != nil {
		pol["partitions_count"] = desc.Stats.Partitions
	}
	partitioningPolicy = append(partitioningPolicy, pol)
	_ = d.Set("partitioning_policy", partitioningPolicy)

	_ = d.Set("primary_key_bloom_filter", desc.KeyBloomFilter == options.FeatureEnabled)
}
