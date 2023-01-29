package topic

import (
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func flattenYDBTopicDescription(d *schema.ResourceData, desc topictypes.TopicDescription) error {
	_ = d.Set("name", d.Get("name").(string)) // NOTE(shmel1k@): TopicService SDK does not return path for stream.
	_ = d.Set("partitions_count", desc.PartitionSettings.MinActivePartitions)
	_ = d.Set("retention_period_ms", desc.RetentionPeriod.Milliseconds())

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

	rules := make([]map[string]interface{}, 0, len(desc.Consumers))
	for _, r := range desc.Consumers {
		var codecs []string
		for _, codec := range r.SupportedCodecs {
			if c, ok := ydbTopicCodecToCodecName[codec]; ok {
				codecs = append(codecs, c)
			}
		}
		rules = append(rules, map[string]interface{}{
			"name":                          r.Name,
			"starting_message_timestamp_ms": r.ReadFrom.UnixMilli(),
			"supported_codecs":              codecs,
		})
	}

	err := d.Set("consumer", rules)
	if err != nil {
		return fmt.Errorf("failed to set consumer %+v: %w", rules, err)
	}

	err = d.Set("supported_codecs", supportedCodecs)
	if err != nil {
		return err
	}

	return d.Set("database_endpoint", d.Get("database_endpoint").(string))
}

func mergeYDBTopicConsumerSettings(
	consumers []interface{},
	readRules []topictypes.Consumer,
) (opts []topicoptions.AlterOption) {
	rules := make(map[string]topictypes.Consumer, len(readRules))
	for i := 0; i < len(readRules); i++ {
		rules[readRules[i].Name] = readRules[i]
	}

	consumersMap := make(map[string]struct{})
	for _, v := range consumers {
		consumer := v.(map[string]interface{})
		consumerName, ok := consumer["name"].(string)
		if !ok {
			continue
		}

		consumersMap[consumerName] = struct{}{}

		supportedCodecs, ok := consumer["supported_codecs"].([]interface{})
		if !ok {
			for _, vv := range ydbTopicAllowedCodecs {
				supportedCodecs = append(supportedCodecs, vv)
			}
		}
		startingMessageTS, ok := consumer["starting_message_timestamp_ms"].(int)
		if !ok {
			startingMessageTS = 0
		}

		r, ok := rules[consumerName]
		if !ok {
			// consumer was deleted by someone outside terraform or does not exist.
			codecs := make([]topictypes.Codec, 0, len(supportedCodecs))
			for _, c := range supportedCodecs {
				codec := c.(string)
				codecs = append(codecs, ydbTopicCodecNameToCodec[strings.ToLower(codec)])
			}
			opts = append(opts, topicoptions.AlterWithAddConsumers(
				topictypes.Consumer{
					Name:            consumerName,
					ReadFrom:        time.UnixMilli(int64(startingMessageTS)),
					SupportedCodecs: codecs,
				},
			))
			continue
		}

		readFrom := time.UnixMilli(int64(startingMessageTS))
		if r.ReadFrom != readFrom {
			opts = append(opts, topicoptions.AlterConsumerWithReadFrom(consumerName, readFrom))
		}

		newCodecs := make([]topictypes.Codec, 0, len(supportedCodecs))
		for _, codec := range supportedCodecs {
			c := ydbTopicCodecNameToCodec[strings.ToLower(codec.(string))]
			newCodecs = append(newCodecs, c)
		}
		if len(newCodecs) != 0 {
			opts = append(opts, topicoptions.AlterConsumerWithSupportedCodecs(consumerName, newCodecs))
		}
	}
	return opts
}

func prepareYDBTopicAlterSettings(
	d *schema.ResourceData,
	settings topictypes.TopicDescription,
) (opts []topicoptions.AlterOption) {
	if d.HasChange("partitions_count") {
		opts = append(opts, topicoptions.AlterWithPartitionCountLimit(int64(d.Get("partitions_count").(int))))
		opts = append(opts, topicoptions.AlterWithMinActivePartitions(int64(d.Get("partitions_count").(int))))
	}
	if d.HasChange("supported_codecs") {
		codecs := d.Get("supported_codecs").([]interface{})
		updatedCodecs := make([]topictypes.Codec, 0, len(codecs))

		for _, c := range codecs {
			cc, ok := ydbTopicCodecNameToCodec[strings.ToLower(c.(string))]
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

	if d.HasChange("consumer") {
		additionalOpts := mergeYDBTopicConsumerSettings(d.Get("consumer").([]interface{}), settings.Consumers)
		opts = append(opts, additionalOpts...)
	}

	return opts
}
