package topic

import (
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
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
	consumers *schema.Set,
	readRules []topictypes.Consumer,
) (opts []topicoptions.AlterOption) {
	var newConsumers []topictypes.Consumer
	rules := make(map[string]topictypes.Consumer, len(readRules))
	for i := 0; i < len(readRules); i++ {
		rules[readRules[i].Name] = readRules[i]
	}

	consumersMap := make(map[string]struct{})
	for _, v := range consumers.List() {
		consumer := v.(map[string]interface{})
		consumerName, ok := consumer["name"].(string)
		if !ok {
			continue
		}

		consumersMap[consumerName] = struct{}{}

		supportedCodecs, ok := consumer["supported_codecs"].(*schema.Set)
		if !ok {
			for _, vv := range YDBTopicAllowedCodecs {
				supportedCodecs.Add(vv)
			}
		}
		startingMessageTS, ok := consumer["starting_message_timestamp_ms"].(int)
		if !ok {
			startingMessageTS = 0
		}

		important, ok := consumer["important"].(bool)
		if !ok {
			important = false
		}

		var availabilityPeriod *time.Duration
		if availabilityPeriodHours, ok := consumer["availability_period_hours"].(int); ok && availabilityPeriodHours > 0 {
			period := time.Duration(availabilityPeriodHours) * time.Hour
			availabilityPeriod = &period
		}

		r, ok := rules[consumerName]
		if !ok {
			// consumer was deleted by someone outside terraform or does not exist.
			codecs := make([]topictypes.Codec, 0, len(supportedCodecs.List()))
			for _, c := range supportedCodecs.List() {
				codec := c.(string)
				codecs = append(codecs, YDBTopicCodecNameToCodec[strings.ToLower(codec)])
			}
			newConsumers = append(newConsumers,
				topictypes.Consumer{
					Name:               consumerName,
					ReadFrom:           time.UnixMilli(int64(startingMessageTS)),
					SupportedCodecs:    codecs,
					Important:          important,
					AvailabilityPeriod: availabilityPeriod,
				},
			)
			continue
		}

		if r.Important != important {
			opts = append(opts, topicoptions.AlterConsumerWithImportant(consumerName, important))
		}

		readFrom := time.UnixMilli(int64(startingMessageTS))
		if r.ReadFrom != readFrom {
			opts = append(opts, topicoptions.AlterConsumerWithReadFrom(consumerName, readFrom))
		}

		// Compare availability_period
		if availabilityPeriod == nil && r.AvailabilityPeriod != nil {
			opts = append(opts, topicoptions.AlterConsumerResetAvailabilityPeriod(consumerName))
		} else if availabilityPeriod != nil && (r.AvailabilityPeriod == nil || *availabilityPeriod != *r.AvailabilityPeriod) {
			opts = append(opts, topicoptions.AlterConsumerWithAvailabilityPeriod(consumerName, *availabilityPeriod))
		}

		newCodecs := make([]topictypes.Codec, 0, len(supportedCodecs.List()))
		for _, codec := range supportedCodecs.List() {
			c := YDBTopicCodecNameToCodec[strings.ToLower(codec.(string))]
			newCodecs = append(newCodecs, c)
		}
		if len(newCodecs) != 0 {
			opts = append(opts, topicoptions.AlterConsumerWithSupportedCodecs(consumerName, newCodecs))
		}
	}
	opts = append(opts, topicoptions.AlterWithAddConsumers(newConsumers...))

	var withDropConsumerNames []string
	for _, r := range rules {
		if _, ok := consumersMap[r.Name]; !ok {
			withDropConsumerNames = append(withDropConsumerNames, r.Name)
		}
	}

	if len(withDropConsumerNames) > 0 {
		opts = append(opts, topicoptions.AlterWithDropConsumers(withDropConsumerNames...))
	}

	return opts
}

func ExpandConsumers(consumers *schema.Set) []topictypes.Consumer {
	result := make([]topictypes.Consumer, 0, len(consumers.List()))
	for _, v := range consumers.List() {
		consumer := v.(map[string]interface{})
		supportedCodecs, ok := consumer["supported_codecs"].(*schema.Set)
		if !ok {
			for _, vv := range YDBTopicAllowedCodecs {
				supportedCodecs.Add(vv)
			}
		}
		consumerName := consumer["name"].(string)
		startingMessageTS, ok := consumer["starting_message_timestamp_ms"].(int)
		if !ok {
			startingMessageTS = 0
		}
		codecs := make([]topictypes.Codec, 0, len(supportedCodecs.List()))
		for _, c := range supportedCodecs.List() {
			codec := c.(string)
			codecs = append(codecs, YDBTopicCodecNameToCodec[strings.ToLower(codec)])
		}
		important, ok := consumer["important"].(bool)
		if !ok {
			important = false
		}
		var availabilityPeriod *time.Duration
		if availabilityPeriodHours, ok := consumer["availability_period_hours"].(int); ok {
			period := time.Duration(availabilityPeriodHours) * time.Hour
			availabilityPeriod = &period
		}
		result = append(result, topictypes.Consumer{
			Name:               consumerName,
			SupportedCodecs:    codecs,
			ReadFrom:           time.UnixMilli(int64(startingMessageTS)),
			Important:          important,
			AvailabilityPeriod: availabilityPeriod,
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

		consumer := map[string]any{
			"name":                          r.Name,
			"starting_message_timestamp_ms": r.ReadFrom.UnixMilli(),
			"supported_codecs":              codecs,
			"important":                     r.Important,
		}

		if r.AvailabilityPeriod != nil {
			consumer["availability_period_hours"] = int64(r.AvailabilityPeriod.Hours())
		}

		cons = append(cons, consumer)
	}

	return cons
}
