package table

import (
	"fmt"
	"net/url"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type Column struct {
	Name    string
	Type    string
	Family  string
	NotNull bool
}

type PrimaryKey struct {
	Columns []string
}

type Index struct {
	Name    string
	Type    string
	Columns []string
	Cover   []string
}

type TTL struct {
	ColumnName     string
	Mode           string
	ExpireInterval string
}

type PartitionAtKeys struct {
	Keys []interface{}
}

type PartitioningSettings struct {
	BySize             *int
	ByLoad             *bool
	PartitionAtKeys    []*PartitionAtKeys
	PartitionsCount    int
	MinPartitionsCount int
	MaxPartitionsCount int
}

type ReplicationSettings struct {
	ReadReplicasSettings string
}

type Family struct {
	Name        string
	Data        string
	Compression string
}

type ChangeDataCaptureSettings struct {
	Mode   string
	Format string
}

type Resource struct {
	Path                 string
	DatabaseEndpoint     string
	Attributes           map[string]string
	Family               []*Family
	Columns              []*Column
	Indexes              []*Index
	PrimaryKey           *PrimaryKey
	TTL                  *TTL
	ReplicationSettings  *ReplicationSettings
	PartitioningSettings *PartitioningSettings
	EnableBloomFilter    *bool
}

func expandTableTTLSettings(d *schema.ResourceData) (ttl *TTL) {
	v, ok := d.GetOk("ttl")
	if !ok {
		return
	}
	ttlSet := v.(*schema.Set)
	for _, l := range ttlSet.List() {
		m := l.(map[string]interface{})
		ttl = &TTL{}
		ttl.ColumnName = m["column_name"].(string)
		//		ttl.Mode = m["mode"].(string)
		ttl.ExpireInterval = m["expire_interval"].(string)
	}
	return
}

func expandTableReplicasSettings(d *schema.ResourceData) (p *ReplicationSettings) {
	v, ok := d.GetOk("read_replicas_settings")
	if !ok {
		return
	}

	p = &ReplicationSettings{}
	p.ReadReplicasSettings = v.(string)
	return
}

func expandPartitionAtKeys(p []interface{}, primaryKeyColumns []*Column) ([]*PartitionAtKeys, error) {
	if len(p) == 0 {
		return nil, nil
	}

	res := make([]*PartitionAtKeys, 0, len(p))
	for _, v := range p {
		vv := v.(map[string]interface{})
		keys := vv["keys"].([]interface{})
		pp := &PartitionAtKeys{}
		for i, k := range keys {
			if i == len(primaryKeyColumns) {
				return nil, fmt.Errorf("can not be more partition keys than primary key columns")
			}
			got, err := parsePartitionKey(k.(string), primaryKeyColumns[i].Type)
			if err != nil {
				return nil, err
			}
			pp.Keys = append(pp.Keys, got)
		}
		res = append(res, pp)
	}
	return res, nil
}

func expandTablePartitioningPolicySettings(d *schema.ResourceData, columns []*Column) (p *PartitioningSettings, err error) {
	v, ok := d.GetOk("partitioning_settings")
	if !ok {
		return
	}

	p = &PartitioningSettings{}

	pSet := v.(*schema.Set)
	for _, l := range pSet.List() {
		m := l.(map[string]interface{})
		if partitionsCount, ok := m["uniform_partitions"].(int); ok {
			p.PartitionsCount = partitionsCount
		}
		if explicitPartitions, ok := m["partition_at_keys"].([]interface{}); ok {
			p.PartitionAtKeys, err = expandPartitionAtKeys(explicitPartitions, columns)
			if err != nil {
				return nil, err
			}
		}
		if minPartitionsCount, ok := m["auto_partitioning_min_partitions_count"].(int); ok {
			p.MinPartitionsCount = minPartitionsCount
		}
		if maxPartitionsCount, ok := m["auto_partitioning_max_partitions_count"].(int); ok {
			p.MaxPartitionsCount = maxPartitionsCount
		}
		if byLoad, ok := m["auto_partitioning_by_load"].(bool); ok {
			p.ByLoad = &byLoad
		}
		if bySize, ok := m["auto_partitioning_by_size_enabled"].(int); ok {
			p.BySize = &bySize
		}
	}

	return p, nil
}

func tableResourceSchemaToTableResource(d *schema.ResourceData) (*Resource, error) {
	columnsRaw := d.Get("column").([]interface{})
	columns := make([]*Column, 0, len(columnsRaw))
	for _, v := range columnsRaw {
		mp := v.(map[string]interface{})
		family := ""
		if f, ok := mp["family"].(string); ok {
			family = f
		}
		col := &Column{
			Name:   mp["name"].(string),
			Type:   mp["type"].(string),
			Family: family,
		}
		if notNull, ok := mp["not_null"]; ok {
			col.NotNull = notNull.(bool)
		}
		columns = append(columns, col)
	}

	pkRaw := d.Get("primary_key").([]interface{})
	pk := make([]string, 0, len(pkRaw))
	for _, v := range pkRaw {
		pk = append(pk, v.(string))
	}

	indexesRaw := d.Get("index")
	var indexes []*Index
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

			var coverArr []string
			if r["covers"] != nil {
				for _, c := range r["covers"].([]interface{}) {
					coverArr = append(coverArr, c.(string))
				}
			}

			indexes = append(indexes, &Index{
				Name:    name,
				Type:    typ,
				Columns: colsArr,
				Cover:   coverArr,
			})
		}
	}
	familiesRaw := d.Get("family")
	var families []*Family
	if familiesRaw != nil {
		raw := familiesRaw.([]interface{})
		for _, rw := range raw {
			r := rw.(map[string]interface{})
			name := r["name"].(string)
			data := r["data"].(string)
			compression := r["compression"].(string)
			families = append(families, &Family{
				Name:        name,
				Data:        data,
				Compression: compression,
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

	databaseEndpoint := d.Get("database_endpoint").(string)
	databaseURL, err := url.Parse(databaseEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database endpoint: %w", err)
	}

	partitioningSettings, err := expandTablePartitioningPolicySettings(d, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to expand table partitioning settings: %s", err)
	}

	replicasSettings := expandTableReplicasSettings(d)

	var bloomFilterEnabled *bool
	if v, ok := d.GetOk("primary_key_bloom_filter"); ok {
		b := v.(bool)
		bloomFilterEnabled = &b
	}

	return &Resource{
		Path:             databaseURL.Query().Get("database") + "/" + d.Get("path").(string),
		DatabaseEndpoint: d.Get("database_endpoint").(string),
		Attributes:       attributes,
		Family:           families,
		Columns:          columns,
		Indexes:          indexes,
		PrimaryKey: &PrimaryKey{
			Columns: pk,
		},
		TTL:                  ttl,
		PartitioningSettings: partitioningSettings,
		ReplicationSettings:  replicasSettings,
		EnableBloomFilter:    bloomFilterEnabled,
	}, nil
}

func flattenTablePartitioningSettings(d *schema.ResourceData, settings options.PartitioningSettings) []interface{} {
	output := make([]interface{}, 0, 1)
	partitioningSettings := make(map[string]interface{})
	if d.HasChange("partitioning_settings.partition_at_keys") {
		oldPartitionAtKeys, _ := d.GetChange("partitioning_settings.partition_at_keys")
		partitioningSettings["partition_at_keys"] = oldPartitionAtKeys
	} else {
		partitioningSettings["partition_at_keys"] = d.Get("partitioning_settings.partition_at_keys")
	}

	if d.HasChange("partitioning_settings.uniform_partitions") {
		oldUniformPartitions, _ := d.GetChange("partitioning_settings.uniform_partitions")
		partitioningSettings["uniform_partitions"] = oldUniformPartitions
	} else {
		partitioningSettings["uniform_partitions"] = d.Get("partitioning_settings.uniform_partitions")
	}
	partitioningSettings["auto_partitioning_by_size_enabled"] = settings.PartitioningBySize == options.FeatureEnabled
	partitioningSettings["auto_partitioning_by_load"] = settings.PartitioningByLoad == options.FeatureEnabled
	partitioningSettings["auto_partitioning_size_mb"] = settings.PartitionSizeMb
	partitioningSettings["auto_partitioning_min_partitions_count"] = settings.MinPartitionsCount
	partitioningSettings["auto_partitioning_max_partitions_count"] = settings.MaxPartitionsCount

	output = append(output, partitioningSettings)
	return output
}

func flattenTableDescription(d *schema.ResourceData, desc options.Description, database string) {
	_ = database
	_ = d.Set("path", desc.Name) // TODO(shmel1k@): path?

	cols := make([]interface{}, 0, len(desc.Columns))
	for _, col := range desc.Columns {
		mp := make(map[string]interface{})
		mp["name"] = col.Name
		mp["type"] = col.Type.String() // TODO(shmel1k@): why optional?
		mp["family"] = col.Family
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

		covers := make([]interface{}, 0, len(idx.DataColumns))
		for _, c := range idx.DataColumns {
			covers = append(covers, c)
		}
		mp["covers"] = covers
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
	_ = d.Set("partitioning_settings", flattenTablePartitioningSettings(d, desc.PartitioningSettings))

	_ = d.Set("primary_key_bloom_filter", desc.KeyBloomFilter == options.FeatureEnabled)
}
