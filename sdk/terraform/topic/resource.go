package topic

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

const (
	ydbTopicCodecGZIP = "gzip"
	ydbTopicCodecRAW  = "raw"
	ydbTopicCodecZSTD = "zstd"
)

const (
	ydbTopicDefaultPartitionsCount        = 2
	ydbTopicDefaultRetentionPeriod        = 1000 * 60 * 60 * 18 // 24 hours
	ydbTopicDefaultMaxPartitionWriteSpeed = 1048576
)

var (
	ydbTopicAllowedCodecs = []string{
		ydbTopicCodecRAW,
		ydbTopicCodecGZIP,
		ydbTopicCodecZSTD,
	}

	ydbTopicDefaultCodecs = []topictypes.Codec{
		topictypes.CodecRaw,
		topictypes.CodecGzip,
		topictypes.CodecZstd,
	}

	ydbTopicCodecNameToCodec = map[string]topictypes.Codec{
		ydbTopicCodecRAW:  topictypes.CodecRaw,
		ydbTopicCodecGZIP: topictypes.CodecGzip,
		ydbTopicCodecZSTD: topictypes.CodecZstd,
	}

	ydbTopicCodecToCodecName = map[topictypes.Codec]string{
		topictypes.CodecRaw:  ydbTopicCodecRAW,
		topictypes.CodecGzip: ydbTopicCodecGZIP,
		topictypes.CodecZstd: ydbTopicCodecZSTD,
	}
)

func (t *Provider) Resource(deprecationMessage string) *schema.Resource {
	r := &schema.Resource{
		CreateContext: t.resourceYDBTopicCreate,
		ReadContext:   t.resourceYDBTopicRead,
		UpdateContext: t.resourceYDBTopicUpdate,
		DeleteContext: t.resourceYDBTopicDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Timeouts: defaultTimeouts(),

		SchemaVersion: 0,

		Schema: map[string]*schema.Schema{
			"database_endpoint": {
				Type:     schema.TypeString,
				Required: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"partitions_count": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  ydbTopicDefaultPartitionsCount,
			},
			"supported_codecs": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type:         schema.TypeString,
					ValidateFunc: validation.StringInSlice(ydbTopicAllowedCodecs, false),
				},
				Computed: true,
			},
			"retention_period_ms": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  ydbTopicDefaultRetentionPeriod,
			},
			"consumer": {
				Type:     schema.TypeList,
				Optional: true,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.NoZeroValues,
						},
						"supported_codecs": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type:         schema.TypeString,
								ValidateFunc: validation.StringInSlice(ydbTopicAllowedCodecs, false),
							},
							Computed: true,
						},
						"starting_message_timestamp_ms": {
							Type:     schema.TypeInt,
							Optional: true,
							Computed: true,
						},
						"service_type": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
			},
		},
	}

	r.DeprecationMessage = deprecationMessage

	return r
}

func (t *Provider) resourceYDBTopicCreate(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	client, err := t.createYDBConnection(ctx, d, nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize yds control plane client: %w", err))
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	var supportedCodecs []topictypes.Codec
	if gotCodecs, ok := d.GetOk("supported_codecs"); !ok {
		supportedCodecs = ydbTopicDefaultCodecs
	} else {
		for _, c := range gotCodecs.([]interface{}) {
			cod := c.(string)
			supportedCodecs = append(supportedCodecs, ydbTopicCodecNameToCodec[cod])
		}
	}

	consumers := make([]topictypes.Consumer, 0, 4)
	for _, v := range d.Get("consumer").([]interface{}) {
		consumer := v.(map[string]interface{})
		supportedCodecs, ok := consumer["supported_codecs"].([]interface{})
		if !ok {
			for _, vv := range ydbTopicAllowedCodecs {
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
			codecs = append(codecs, ydbTopicCodecNameToCodec[strings.ToLower(codec)])
		}
		consumers = append(consumers, topictypes.Consumer{
			Name:            consumerName,
			SupportedCodecs: codecs,
			ReadFrom:        time.Unix(int64(startingMessageTS/1000), 0),
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("failed to create consumer %q: %w", consumerName, err))
		}
	}

	err = client.Topic().Create(ctx, d.Get("name").(string),
		topicoptions.CreateWithSupportedCodecs(supportedCodecs...),
		topicoptions.CreateWithPartitionWriteBurstBytes(ydbTopicDefaultMaxPartitionWriteSpeed),
		topicoptions.CreateWithPartitionWriteSpeedBytesPerSecond(ydbTopicDefaultMaxPartitionWriteSpeed),
		topicoptions.CreateWithRetentionPeriod(time.Duration(d.Get("retention_period_ms").(int))*time.Millisecond),
		topicoptions.CreateWithMinActivePartitions(int64(d.Get("partitions_count").(int))),
		topicoptions.CreateWithConsumer(consumers...),
	)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-topic control plane client: %w", err))
	}

	topicPath := d.Get("name").(string)
	d.SetId(d.Get("database_endpoint").(string) + "/" + topicPath)

	return t.resourceYDBTopicRead(ctx, d, nil)
}

func (t *Provider) resourceYDBTopicUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return t.performYDBTopicUpdate(ctx, d)
}

func (t *Provider) resourceYDBTopicDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	topic, err := parseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	client, err := t.createYDBConnection(ctx, d, topic)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-topic control plane client: %w", err))
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	topicName := topic.getEntityPath()
	err = client.Topic().Drop(ctx, topicName)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to delete topic: %w", err))
	}

	return nil
}

type ResourceDataProxy interface {
	Get(key string) interface{}
	GetOk(key string) (interface{}, bool)

	// GetOkExists and methods below are bypassed (i.e. call schema.ResourceData directly)
	// Deprecated: calls a deprecated method
	GetOkExists(key string) (interface{}, bool)

	Id() string
	SetId(id string)
	Set(key string, value interface{}) error
	HasChange(key string) bool
	GetChange(key string) (interface{}, interface{})
	Partial(on bool)
	Timeout(s string) time.Duration
}

func (t *Provider) createYDBConnection(
	ctx context.Context,
	d ResourceDataProxy,
	ydbEn *ydbEntity,
) (ydb.Connection, error) {
	var databaseEndpoint string
	if ydbEn != nil {
		databaseEndpoint = ydbEn.prepareFullYDBEndpoint()
	} else {
		// NOTE(shmel1k@): resource is not initialized yet.
		databaseEndpoint = d.Get("database_endpoint").(string)
	}

	token, err := t.tokenCallback(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ydb token: %w", err)
	}

	sess, err := ydb.Open(ctx, databaseEndpoint, ydb.WithAccessTokenCredentials(token))
	if err != nil {
		return nil, fmt.Errorf("failed to create control-plane client: %w", err)
	}
	return sess, nil
}

func (t *Provider) resourceYDBTopicRead(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	topic, err := parseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	ydbClient, err := t.createYDBConnection(ctx, d, topic)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-topic control plane client: %w", err))
	}
	defer func() {
		_ = ydbClient.Close(ctx)
	}()
	topicClient := ydbClient.Topic()

	topicName := topic.getEntityPath()

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

func (t *Provider) performYDBTopicUpdate(ctx context.Context, d *schema.ResourceData) diag.Diagnostics {
	topic, err := parseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	ydbClient, err := t.createYDBConnection(ctx, d, topic)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to initialize ydb-topic control plane client: %w", err))
	}
	defer func() {
		_ = ydbClient.Close(ctx)
	}()

	topicClient := ydbClient.Topic()

	if d.HasChange("name") {
		// Creating new topic
		return t.resourceYDBTopicCreate(ctx, d, nil)
	}

	topicName := topic.getEntityPath()
	desc, err := topicClient.Describe(ctx, topicName)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return t.resourceYDBTopicCreate(ctx, d, nil)
		}
		return diag.FromErr(fmt.Errorf("failed to get description for topic %q", topicName))
	}

	opts := prepareYDBTopicAlterSettings(d, desc)

	err = topicClient.Alter(ctx, topicName, opts...)
	if err != nil {
		return diag.FromErr(fmt.Errorf("got error when tried to alter topic: %w", err))
	}

	return t.resourceYDBTopicRead(ctx, d, nil)
}
