package topic

import (
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func flattenYDBTopicConsumerDescription(d *schema.ResourceData, desc topictypes.TopicDescription) error {
	_ = d.Set("name", d.Get("name").(string)) // NOTE(shmel1k@): TopicService SDK does not return path for stream.

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

	err := d.Set("supported_codecs", supportedCodecs)
	if err != nil {
		return err
	}

	return d.Set("database_endpoint", d.Get("database_endpoint").(string))
}

func prepareYDBTopicConsumerAlterSettings(
	d *schema.ResourceData,
	settings topictypes.TopicDescription,
) (opts []topicoptions.AlterOption) {
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
	if d.HasChange("starting_message_timestamp_ms") {
		opts = append(opts, topicoptions.AlterWithRetentionPeriod(time.Duration(d.Get("starting_message_timestamp_ms").(int))*time.Millisecond))
	}

	return opts
}
