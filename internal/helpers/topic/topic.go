package topic

import (
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

const (
	YDBTopicCodecGZIP = "gzip"
	YDBTopicCodecRAW  = "raw"
	YDBTopicCodecZSTD = "zstd"
)

var (
	YDBTopicAllowedCodecs = []string{
		YDBTopicCodecRAW,
		YDBTopicCodecGZIP,
		YDBTopicCodecZSTD,
	}

	YDBTopicDefaultCodecs = []topictypes.Codec{
		topictypes.CodecRaw,
		topictypes.CodecGzip,
		topictypes.CodecZstd,
	}

	YDBTopicCodecNameToCodec = map[string]topictypes.Codec{
		YDBTopicCodecRAW:  topictypes.CodecRaw,
		YDBTopicCodecGZIP: topictypes.CodecGzip,
		YDBTopicCodecZSTD: topictypes.CodecZstd,
	}

	YDBTopicCodecToCodecName = map[topictypes.Codec]string{
		topictypes.CodecRaw:  YDBTopicCodecRAW,
		topictypes.CodecGzip: YDBTopicCodecGZIP,
		topictypes.CodecZstd: YDBTopicCodecZSTD,
	}
)

func MergeConsumerSettings(
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
			for _, vv := range YDBTopicAllowedCodecs {
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
				codecs = append(codecs, YDBTopicCodecNameToCodec[strings.ToLower(codec)])
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
			c := YDBTopicCodecNameToCodec[strings.ToLower(codec.(string))]
			newCodecs = append(newCodecs, c)
		}
		if len(newCodecs) != 0 {
			opts = append(opts, topicoptions.AlterConsumerWithSupportedCodecs(consumerName, newCodecs))
		}
	}
	return opts
}

func ExpandConsumers(consumers []interface{}) []topictypes.Consumer {
	result := make([]topictypes.Consumer, 0, len(consumers))
	for _, v := range consumers {
		consumer := v.(map[string]interface{})
		supportedCodecs, ok := consumer["supported_codecs"].([]interface{})
		if !ok {
			for _, vv := range YDBTopicAllowedCodecs {
				supportedCodecs = append(supportedCodecs, vv)
			}
		}
		consumerName := consumer["name"].(string)
		startingMessageTS, ok := consumer["starting_message_timestamp_ms"].(int)
		if !ok {
			startingMessageTS = 0
		}
		codecs := make([]topictypes.Codec, 0, len(supportedCodecs))
		for _, c := range supportedCodecs {
			codec := c.(string)
			codecs = append(codecs, YDBTopicCodecNameToCodec[strings.ToLower(codec)])
		}
		result = append(result, topictypes.Consumer{
			Name:            consumerName,
			SupportedCodecs: codecs,
			ReadFrom:        time.Unix(int64(startingMessageTS/1000), 0),
		})
	}

	return result
}

func FlattenConsumersDescription(consumers []topictypes.Consumer) []map[string]interface{} {
	cons := make([]map[string]interface{}, 0, len(consumers))
	for _, r := range consumers {
		var codecs []string
		for _, codec := range r.SupportedCodecs {
			if c, ok := YDBTopicCodecToCodecName[codec]; ok {
				codecs = append(codecs, c)
			}
		}
		cons = append(cons, map[string]interface{}{
			"name":                          r.Name,
			"starting_message_timestamp_ms": r.ReadFrom.UnixMilli(),
			"supported_codecs":              codecs,
		})
	}

	return cons
}
