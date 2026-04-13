package externaltable

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

type ColumnDef struct {
	Name    string
	Type    string
	NotNull bool
}

type Resource struct {
	Entity           *helpers.YDBEntity
	FullPath         string
	Path             string
	DatabaseEndpoint string
	DataSourcePath   string
	Location         string
	Format           string
	Compression      string
	Columns          []ColumnDef
}

func (r *Resource) getConnectionString() string {
	if r.DatabaseEndpoint != "" {
		return r.DatabaseEndpoint
	}
	return r.Entity.PrepareFullYDBEndpoint()
}

func resourceSchemaToResource(d *schema.ResourceData) (*Resource, error) {
	var entity *helpers.YDBEntity
	var err error
	if d.Id() != "" {
		entity, err = helpers.ParseYDBEntityID(d.Id())
		if err != nil {
			return nil, fmt.Errorf("failed to parse external table entity: %w", err)
		}
	}

	databaseEndpoint := d.Get("connection_string").(string)
	var path string
	if entity != nil {
		path = entity.GetEntityPath()
		databaseEndpoint = entity.PrepareFullYDBEndpoint()
	} else {
		databaseURL, err := url.Parse(databaseEndpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database endpoint: %w", err)
		}
		path = d.Get("path").(string)
		_ = databaseURL
	}

	columns := parseColumns(d)

	return &Resource{
		Entity:           entity,
		FullPath:         path,
		Path:             helpers.TrimPath(d.Get("path").(string)),
		DatabaseEndpoint: databaseEndpoint,
		DataSourcePath:   d.Get("data_source_path").(string),
		Location:         d.Get("location").(string),
		Format:           d.Get("format").(string),
		Compression:      d.Get("compression").(string),
		Columns:          columns,
	}, nil
}

func parseColumns(d *schema.ResourceData) []ColumnDef {
	rawColumns := d.Get("column").([]interface{})
	columns := make([]ColumnDef, 0, len(rawColumns))
	for _, raw := range rawColumns {
		m := raw.(map[string]interface{})
		columns = append(columns, ColumnDef{
			Name:    m["name"].(string),
			Type:    m["type"].(string),
			NotNull: m["not_null"].(bool),
		})
	}
	return columns
}

func unwrapType(t types.Type) (typ string, notNull bool) {
	yqlStr := t.Yql()
	notNull = true

	if strings.HasPrefix(yqlStr, "Optional<") {
		notNull = false
		yqlStr = strings.TrimPrefix(yqlStr, "Optional<")
		yqlStr = strings.TrimSuffix(yqlStr, ">")
	}

	return yqlStr, notNull
}

func flattenDescription(d *schema.ResourceData, entity *helpers.YDBEntity, desc *options.ExternalTableDescription) error {
	if err := d.Set("path", entity.GetEntityPath()); err != nil {
		return err
	}
	if err := d.Set("connection_string", entity.PrepareFullYDBEndpoint()); err != nil {
		return err
	}
	if err := d.Set("data_source_path", desc.DataSourcePath); err != nil {
		return err
	}
	if err := d.Set("location", desc.Location); err != nil {
		return err
	}

	if v, ok := desc.Content["FORMAT"]; ok {
		if err := d.Set("format", v); err != nil {
			return err
		}
	}
	if v, ok := desc.Content["COMPRESSION"]; ok {
		if err := d.Set("compression", v); err != nil {
			return err
		}
	}

	cols := make([]interface{}, 0, len(desc.Columns))
	for _, col := range desc.Columns {
		mp := make(map[string]interface{})
		mp["name"] = col.Name
		mp["type"], mp["not_null"] = unwrapType(col.Type)
		cols = append(cols, mp)
	}
	return d.Set("column", cols)
}
