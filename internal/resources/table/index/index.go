package index

import (
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/internal/resources"
)

type handler struct {
	token string
}

type resource struct {
	TablePath        string
	ConnectionString string
	Name             string
	Type             string
	Columns          []string
	Cover            []string
	Entity           *helpers.YDBEntity
}

func indexResourceSchemaToIndexResource(d *schema.ResourceData) (*resource, error) {
	var entity *helpers.YDBEntity
	var err error
	if d.Id() != "" {
		entity, err = helpers.ParseYDBEntityID(d.Id())
		if err != nil {
			return nil, fmt.Errorf("failed to parse index entity: %w", err)
		}
	}
	tablePath := d.Get("table_path").(string)
	connectionString := d.Get("connection_string").(string)
	name := d.Get("name").(string)
	typ := d.Get("type").(string)
	colsRaw := d.Get("columns").([]interface{})
	colsArr := make([]string, 0, len(colsRaw))
	for _, c := range colsRaw {
		colsArr = append(colsArr, c.(string))
	}

	var coverArr []string
	if cover, ok := d.GetOk("cover"); ok {
		for _, c := range cover.([]interface{}) {
			coverArr = append(coverArr, c.(string))
		}
	}

	return &resource{
		TablePath:        tablePath,
		ConnectionString: connectionString,
		Name:             name,
		Type:             typ,
		Columns:          colsArr,
		Cover:            coverArr,
		Entity:           entity,
	}, nil
}

func NewHandler(token string) resources.Handler {
	return &handler{
		token: token,
	}
}

func flattenIndexDescription(
	d *schema.ResourceData,
	tablePath string,
	indexDescription options.IndexDescription,
	connectionString string,
) {
	_ = d.Set("table_path", tablePath)
	_ = d.Set("connection_string", connectionString)
	// TODO(shmel1k@): index type?
	_ = d.Set("name", indexDescription.Name)
	cols := make([]interface{}, 0, len(indexDescription.IndexColumns))
	for _, c := range indexDescription.IndexColumns {
		cols = append(cols, c)
	}
	_ = d.Set("columns", cols)
	covers := make([]interface{}, 0, len(indexDescription.DataColumns))
	for _, c := range indexDescription.DataColumns {
		covers = append(covers, c)
	}
	_ = d.Set("cover", covers)
}

func parseTablePathFromIndexEntity(entityPath string) string {
	split := strings.Split(entityPath, "/")
	return strings.Join(split[:len(split)-1], "/")
}
