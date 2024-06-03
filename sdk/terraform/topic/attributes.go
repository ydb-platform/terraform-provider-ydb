package topic

const (
	attributePartitionsCount                    = "partitions_count"
	attributeMeteringMode                       = "metering_mode"
	attributeSupportedCodecs                    = "supported_codecs"
	attributeRetentionPeriodHours               = "retention_period_hours"
	attributeRetentionStorageMB                 = "retention_storage_mb"
	attributePartitionWriteSpeedKBPS            = "partition_write_speed_kbps"
	attributeConsumer                           = "consumer"
	attributeName                               = "name" // NOTE(shmel1k@): deprecated, use 'attributes.Path' instead.
	attributeConsumerStartingMessageTimestampMS = "starting_message_timestamp_ms"
	attributeConsumerImportant                  = "important"
)
