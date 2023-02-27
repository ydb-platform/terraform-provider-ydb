package topic

import (
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
