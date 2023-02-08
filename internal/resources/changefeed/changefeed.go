package changefeed

import (
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

var (
	changefeedModeToStringMap = map[options.ChangefeedMode]string{
		options.ChangefeedModeUnspecified:     "",
		options.ChangefeedModeKeysOnly:        "KEYS_ONLY",
		options.ChangefeedModeNewImage:        "NEW_IMAGE",
		options.ChangefeedModeOldImage:        "OLD_IMAGE",
		options.ChangefeedModeNewAndOldImages: "NEW_AND_OLD_IMAGES",
		options.ChangefeedModeUpdates:         "UPDATES",
	}

	changefeedFormatToStringMap = map[options.ChangefeedFormat]string{
		options.ChangefeedFormatUnspecified: "",
		options.ChangefeedFormatJSON:        "JSON",
	}
)

type ChangeDataCaptureSettings struct {
	DatabaseEndpoint  string
	TablePath         string
	Name              string
	Mode              string
	Format            *string
	RetentionPeriod   *string
	VirtualTimestamps *bool
	Entity            *helpers.YDBEntity
	Consumers         []topictypes.Consumer
}

func changefeedResourceSchemaToChangefeedResource(d *schema.ResourceData) (*ChangeDataCaptureSettings, error) {
	var entity *helpers.YDBEntity
	var err error
	if d.Id() != "" {
		entity, err = helpers.ParseYDBEntityID(d.Id())
		if err != nil {
			return nil, fmt.Errorf("failed to parse changefeed entity: %s", err)
		}
	}

	settings := &ChangeDataCaptureSettings{
		Entity:           entity,
		DatabaseEndpoint: d.Get("database_endpoint").(string),
		Name:             d.Get("name").(string),
		Mode:             d.Get("mode").(string),
		TablePath:        d.Get("table_path").(string),
	}
	if format, ok := d.Get("format").(string); ok && format != "" {
		settings.Format = &format
	}
	if virtualTimestamps, ok := d.Get("virtual_timestamps").(bool); ok {
		settings.VirtualTimestamps = &virtualTimestamps
	}
	if retentionPeriod, ok := d.Get("retention_period").(string); ok && retentionPeriod != "" {
		settings.RetentionPeriod = &retentionPeriod
	}
	settings.Consumers = topic.ExpandConsumers(d.Get("consumer").([]interface{}))

	return settings, nil
}

func flattenCDCDescription(
	d *schema.ResourceData,
	tablePath string,
	cdcDescription options.ChangefeedDescription,
	databaseEndpoint string,
	consumers []topictypes.Consumer,
) {
	_ = d.Set("table_path", tablePath)
	_ = d.Set("database_endpoint", databaseEndpoint)
	_ = d.Set("name", cdcDescription.Name)
	_ = d.Set("mode", changefeedModeToStringMap[cdcDescription.Mode])
	_ = d.Set("format", changefeedFormatToStringMap[cdcDescription.Format])
	_ = d.Set("consumer", topic.FlattenConsumersDescription(consumers))
}

func parseTablePathFromCDCEntity(entityPath string) string {
	split := strings.Split(entityPath, "/")
	return strings.Join(split[:len(split)-1], "/")
}
