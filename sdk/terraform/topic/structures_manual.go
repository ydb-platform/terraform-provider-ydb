package topic

import (
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers/topic"
)

func flattenYDBTopicDescription(d *schema.ResourceData, desc topictypes.TopicDescription) error {
	_ = d.Set("name", d.Get("name").(string)) // NOTE(shmel1k@): TopicService SDK does not return path for stream.
	_ = d.Set("partitions_count", desc.PartitionSettings.MinActivePartitions)
	_ = d.Set("retention_period_ms", desc.RetentionPeriod.Milliseconds())
	_ = d.Set("retention_storage_mb", desc.RetentionStorageMB)
	_ = d.Set("metering_mode", MeteringModeToString(desc.MeteringMode))

	supportedCodecs := make([]string, 0, len(desc.SupportedCodecs))
	for _, v := range desc.SupportedCodecs {
		switch v {
		case topictypes.CodecRaw:
			supportedCodecs = append(supportedCodecs, ydbTopicCodecRAW)
		case topictypes.CodecZstd:
			supportedCodecs = append(supportedCodecs, ydbTopicCodecZSTD)
		case topictypes.CodecGzip:
			supportedCodecs = append(supportedCodecs, ydbTopicCodecGZIP)
		}
	}

	consumers := topic.FlattenConsumersDescription(desc.Consumers)
	err := d.Set("consumer", consumers)
	if err != nil {
		return fmt.Errorf("failed to set consumer %+v: %w", consumers, err)
	}

	err = d.Set("supported_codecs", supportedCodecs)
	if err != nil {
		return err
	}

	return d.Set("database_endpoint", d.Get("database_endpoint").(string))
}

func prepareYDBTopicAlterSettings(
	d *schema.ResourceData,
	settings topictypes.TopicDescription,
) (opts []topicoptions.AlterOption) {
	if d.HasChange("partitions_count") {
		opts = append(opts, topicoptions.AlterWithPartitionCountLimit(int64(d.Get("partitions_count").(int))))
		opts = append(opts, topicoptions.AlterWithMinActivePartitions(int64(d.Get("partitions_count").(int))))
	}
	if d.HasChange("metering_mode") {
		opts = append(opts, topicoptions.AlterWithMeteringMode(StringToMeteringMode(d.Get("metering_mode").(string))))
	}
	if d.HasChange("supported_codecs") {
		codecs := d.Get("supported_codecs").([]interface{})
		updatedCodecs := make([]topictypes.Codec, 0, len(codecs))

		for _, c := range codecs {
			cc, ok := topic.YDBTopicCodecNameToCodec[strings.ToLower(c.(string))]
			if !ok {
				panic(fmt.Sprintf("Unsupported codec %q found after validation", cc))
			}
			updatedCodecs = append(updatedCodecs, cc)
		}
		opts = append(opts, topicoptions.AlterWithSupportedCodecs(updatedCodecs...))
	}
	if d.HasChange("retention_period_ms") {
		opts = append(opts, topicoptions.AlterWithRetentionPeriod(time.Duration(d.Get("retention_period_ms").(int))*time.Millisecond))
	}
	if d.HasChange("retention_storage_mb") {
		opts = append(opts, topicoptions.AlterWithRetentionStorageMB(int64(d.Get("retention_storage_mb").(int))))
	}

	if d.HasChange("consumer") {
		additionalOpts := topic.MergeConsumerSettings(d.Get("consumer").([]interface{}), settings.Consumers)
		opts = append(opts, additionalOpts...)
	}

	return opts
}
