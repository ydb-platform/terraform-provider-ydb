package topic

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers/topic"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/attributes"
)

type caller struct {
	token string
}

const (
	meteringModeRequestUnits     = "request_units"
	meteringModeReservedCapacity = "reserved_capacity"
	meteringModeUnspecified      = "unspecified"
)

func (c *caller) createYDBConnection(
	ctx context.Context,
	d helpers.ResourceDataProxy,
	ydbEn *helpers.YDBEntity,
) (ydb.Connection, error) {
	// TODO(shmel1k@): move to other level.
	var databaseEndpoint string
	if ydbEn != nil {
		databaseEndpoint = ydbEn.PrepareFullYDBEndpoint()
	} else {
		// NOTE(shmel1k@): resource is not initialized yet.
		databaseEndpoint = d.Get(attributes.DatabaseEndpoint).(string)
	}

	sess, err := ydb.Open(ctx, databaseEndpoint, ydb.WithAccessTokenCredentials(c.token))
	if err != nil {
		return nil, fmt.Errorf("failed to create control-plane client: %w", err)
	}
	return sess, nil
}

func (c *caller) performYDBTopicUpdate(ctx context.Context, d *schema.ResourceData) diag.Diagnostics {
	topic, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	ydbClient, err := c.createYDBConnection(ctx, d, topic)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-topic control plane client: %w", err))
	}
	defer func() {
		_ = ydbClient.Close(ctx)
	}()

	topicClient := ydbClient.Topic()

	topicName := topic.GetEntityPath()
	desc, err := topicClient.Describe(ctx, topicName)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return c.resourceYDBTopicCreate(ctx, d, nil)
		}
		return diag.FromErr(fmt.Errorf("failed to get description for topic %q", topicName))
	}

	opts := prepareYDBTopicAlterSettings(d, desc)
	err = topicClient.Alter(ctx, topicName, opts...)
	if err != nil {
		return diag.FromErr(fmt.Errorf("got error when tried to alter topic: %w", err))
	}

	return c.resourceYDBTopicRead(ctx, d, nil)
}

func MeteringModeToString(mode topictypes.MeteringMode) string {
	if mode == topictypes.MeteringModeRequestUnits {
		return meteringModeRequestUnits
	}
	if mode == topictypes.MeteringModeReservedCapacity {
		return meteringModeReservedCapacity
	}
	return meteringModeUnspecified
}

func (c *caller) resourceYDBTopicRead(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	topic, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	ydbClient, err := c.createYDBConnection(ctx, d, topic)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-topic control plane client: %w", err))
	}
	defer func() {
		_ = ydbClient.Close(ctx)
	}()
	topicClient := ydbClient.Topic()

	topicName := topic.GetEntityPath()

	description, err := topicClient.Describe(ctx, topicName)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			d.SetId("") // marking as non-existing resource.
			return nil
		}
		return diag.FromErr(fmt.Errorf("resource: failed to describe topic: %w", err))
	}
	err = flattenYDBTopicDescription(d, description)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to flatten topic description: %w", err))
	}

	return nil
}

func StringToMeteringMode(mode string) topictypes.MeteringMode {
	if mode == meteringModeRequestUnits {
		return topictypes.MeteringModeRequestUnits
	}
	if mode == meteringModeReservedCapacity {
		return topictypes.MeteringModeReservedCapacity
	}
	return topictypes.MeteringModeUnspecified
}

func (c *caller) resourceYDBTopicCreate(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	client, err := c.createYDBConnection(ctx, d, nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize yds control plane client: %w", err))
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	var supportedCodecs []topictypes.Codec
	if gotCodecs, ok := d.GetOk(attributeSupportedCodecs); !ok {
		supportedCodecs = topic.YDBTopicDefaultCodecs
	} else {
		for _, c := range gotCodecs.([]interface{}) {
			cod := c.(string)
			supportedCodecs = append(supportedCodecs, topic.YDBTopicCodecNameToCodec[cod])
		}
	}

	consumers := topic.ExpandConsumers(d.Get(attributeConsumer).([]interface{}))
	options := []topicoptions.CreateOption{
		topicoptions.CreateWithSupportedCodecs(supportedCodecs...),
		topicoptions.CreateWithMinActivePartitions(int64(d.Get(attributePartitionsCount).(int))),
		topicoptions.CreateWithConsumer(consumers...),
	}
	if d.Get(attributeRetentionPeriodHours) != 0 {
		options = append(options, topicoptions.CreateWithRetentionPeriod(time.Duration(d.Get(attributeRetentionPeriodHours).(int))*time.Hour))
	}
	if d.Get(attributeRetentionStorageMB) != 0 {
		options = append(options, topicoptions.CreateWithRetentionStorageMB(int64(d.Get(attributeRetentionStorageMB).(int))))
	}
	if d.Get(attributeMeteringMode) != "" {
		options = append(options, topicoptions.CreateWithMeteringMode(StringToMeteringMode(d.Get(attributeMeteringMode).(string))))
	}
	if d.Get(attributePartitionWriteSpeedKBPS) != 0 {
		writeSpeed := 1024 * d.Get(attributePartitionWriteSpeedKBPS).(int)
		options = append(options, topicoptions.CreateWithPartitionWriteBurstBytes(int64(writeSpeed)))
		options = append(options, topicoptions.CreateWithPartitionWriteSpeedBytesPerSecond(int64(writeSpeed)))
	}
	err = client.Topic().Create(ctx, d.Get(attributeName).(string), options...)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-topic control plane client: %w", err))
	}

	topicPath := d.Get(attributeName).(string)
	d.SetId(d.Get(attributes.DatabaseEndpoint).(string) + "?path=" + topicPath)

	return c.resourceYDBTopicRead(ctx, d, nil)
}

func (c *caller) resourceYDBTopicUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	_ = meta
	return c.performYDBTopicUpdate(ctx, d)
}

func (c *caller) resourceYDBTopicDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	_ = meta
	topic, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	client, err := c.createYDBConnection(ctx, d, topic)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-topic control plane client: %w", err))
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	topicName := topic.GetEntityPath()
	err = client.Topic().Drop(ctx, topicName)
	return diag.FromErr(err)
}
