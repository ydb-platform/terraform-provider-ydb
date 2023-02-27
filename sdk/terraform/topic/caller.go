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
)

type caller struct {
	token string
}

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
		databaseEndpoint = d.Get("database_endpoint").(string)
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

	if d.HasChange("name") {
		// Creating new topic
		return c.resourceYDBTopicCreate(ctx, d, nil)
	}

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

func (c *caller) resourceYDBTopicCreate(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	client, err := c.createYDBConnection(ctx, d, nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize yds control plane client: %w", err))
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	var supportedCodecs []topictypes.Codec
	if gotCodecs, ok := d.GetOk("supported_codecs"); !ok {
		supportedCodecs = topic.YDBTopicDefaultCodecs
	} else {
		for _, c := range gotCodecs.([]interface{}) {
			cod := c.(string)
			supportedCodecs = append(supportedCodecs, topic.YDBTopicCodecNameToCodec[cod])
		}
	}

	err = client.Topic().Create(ctx, d.Get("name").(string),
		topicoptions.CreateWithSupportedCodecs(supportedCodecs...),
		topicoptions.CreateWithPartitionWriteBurstBytes(ydbTopicDefaultMaxPartitionWriteSpeed),
		topicoptions.CreateWithPartitionWriteSpeedBytesPerSecond(ydbTopicDefaultMaxPartitionWriteSpeed),
		topicoptions.CreateWithRetentionPeriod(time.Duration(d.Get("retention_period_ms").(int))*time.Millisecond),
		topicoptions.CreateWithMinActivePartitions(int64(d.Get("partitions_count").(int))),
	)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-topic control plane client: %w", err))
	}

	topicPath := d.Get("name").(string)
	d.SetId(d.Get("database_endpoint").(string) + "&path=" + topicPath)

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
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to delete topic: %w", err))
	}

	return nil
}

func (c *caller) resourceYDBTopicConsumerRead(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
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

	err = flattenYDBTopicConsumerDescription(d, description)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to flatten topic description: %w", err))
	}

	return nil
}

func (c *caller) resourceYDBTopicConsumerCreate(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	client, err := c.createYDBConnection(ctx, d, nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize yds control plane client: %w", err))
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	var supportedCodecs []topictypes.Codec
	if gotCodecs, ok := d.GetOk("supported_codecs"); !ok {
		supportedCodecs = topic.YDBTopicDefaultCodecs
	} else {
		for _, c := range gotCodecs.([]interface{}) {
			cod := c.(string)
			supportedCodecs = append(supportedCodecs, topic.YDBTopicCodecNameToCodec[cod])
		}
	}

	err = client.Topic().Create(ctx, d.Get("name").(string),
		topicoptions.CreateWithSupportedCodecs(supportedCodecs...),
		topicoptions.CreateWithPartitionWriteBurstBytes(ydbTopicDefaultMaxPartitionWriteSpeed),
		topicoptions.CreateWithPartitionWriteSpeedBytesPerSecond(ydbTopicDefaultMaxPartitionWriteSpeed),
		topicoptions.CreateWithRetentionPeriod(time.Duration(d.Get("starting_message_timestamp_ms").(int))*time.Millisecond),
	)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-topic-consumer control plane client: %w", err))
	}

	topicPath := d.Get("name").(string)
	d.SetId(d.Get("database_endpoint").(string) + "&path=" + topicPath)

	return c.resourceYDBTopicConsumerRead(ctx, d, nil)
}

func (c *caller) performYDBTopicConsumerUpdate(ctx context.Context, d *schema.ResourceData) diag.Diagnostics {
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

	if d.HasChange("name") {
		// Creating new topic
		return c.resourceYDBTopicCreate(ctx, d, nil)
	}

	topicName := topic.GetEntityPath()
	desc, err := topicClient.Describe(ctx, topicName)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return c.resourceYDBTopicCreate(ctx, d, nil)
		}
		return diag.FromErr(fmt.Errorf("failed to get description for topic %q", topicName))
	}

	opts := prepareYDBTopicConsumerAlterSettings(d, desc)

	err = topicClient.Alter(ctx, topicName, opts...)
	if err != nil {
		return diag.FromErr(fmt.Errorf("got error when tried to alter topic: %w", err))
	}

	return c.resourceYDBTopicConsumerRead(ctx, d, nil)
}

func (c *caller) resourceYDBTopicConsumerUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	_ = meta
	return c.performYDBTopicConsumerUpdate(ctx, d)
}

func (c *caller) resourceYDBTopicConsumerDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
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
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to delete topic: %w", err))
	}

	return nil
}
