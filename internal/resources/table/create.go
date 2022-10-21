package table

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	tbl "github.com/ydb/terraform-provider-ydb/internal/table"
)

func stringToYDBType(t string) types.Type {
	// TODO(shmel1k@): ask about it.
	switch t {
	case "Optional<Bool>":
		return types.Optional(types.TypeBool)
	case "Optional<Int8>":
		return types.Optional(types.TypeInt8)
	case "Optional<Uint8>":
		return types.Optional(types.TypeUint8)
	case "Optional<Int16>":
		return types.Optional(types.TypeInt16)
	case "Optional<Uint16>":
		return types.Optional(types.TypeUint16)
	case "Optional<Int32>":
		return types.Optional(types.TypeInt32)
	case "Optional<Uint32>":
		return types.Optional(types.TypeUint32)
	case "Optional<Int64>":
		return types.Optional(types.TypeInt64)
	case "Optional<Uint64>":
		return types.Optional(types.TypeUint64)
	case "Optional<Float>":
		return types.Optional(types.TypeFloat)
	case "Optional<Double>":
		return types.Optional(types.TypeDouble)
	case "Optional<Date>":
		return types.Optional(types.TypeDate)
	case "Optional<Datetime>":
		return types.Optional(types.TypeDatetime)
	case "Optional<Timestamp>":
		return types.Optional(types.TypeTimestamp)
	case "Optional<Interval>":
		return types.Optional(types.TypeInterval)
	case "Optional<TzDate>":
		return types.Optional(types.TypeTzDate)
	case "Optional<TzDatetime>":
		return types.Optional(types.TypeTzDatetime)
	case "Optional<TzTimestamp>":
		return types.Optional(types.TypeTzTimestamp)
	case "Optional<String>":
		return types.Optional(types.TypeString)
	case "Optional<Utf8>":
		return types.Optional(types.TypeUTF8)
	case "Optional<YSON>":
		return types.Optional(types.TypeYSON)
	case "Optional<JSON>":
		return types.Optional(types.TypeJSON)
	case "Optional<UUID>":
		return types.Optional(types.TypeUUID)
	case "Optional<JSONDocument>":
		return types.Optional(types.TypeJSONDocument)
	case "Optional<DyNumber>":
		return types.Optional(types.TypeDyNumber)
	case "Bool":
		return types.TypeBool
	case "Int8":
		return types.TypeInt8
	case "Uint8":
		return types.TypeUint8
	case "Int16":
		return types.TypeInt16
	case "Uint16":
		return types.TypeUint16
	case "Int32":
		return types.TypeInt32
	case "Uint32":
		return types.TypeUint32
	case "Int64":
		return types.TypeInt64
	case "Uint64":
		return types.TypeUint64
	case "Float":
		return types.TypeFloat
	case "Double":
		return types.TypeDouble
	case "Date":
		return types.TypeDate
	case "Datetime":
		return types.TypeDatetime
	case "Timestamp":
		return types.TypeTimestamp
	case "Interval":
		return types.TypeInterval
	case "TzDate":
		return types.TypeTzDate
	case "TzDatetime":
		return types.TypeTzDatetime
	case "TzTimestamp":
		return types.TypeTzTimestamp
	case "String":
		return types.TypeString
	case "Utf8":
		return types.TypeUTF8
	case "YSON":
		return types.TypeYSON
	case "JSON":
		return types.TypeJSON
	case "UUID":
		return types.TypeUUID
	case "JSONDocument":
		return types.TypeJSONDocument
	case "DyNumber":
		return types.TypeDyNumber
	default:
		return types.Optional(types.TypeUnknown)
	}
}

func strColumnUnitToYDBColumnUnit(cu string) options.TimeToLiveUnit {
	if cu == "ns" {
		return options.TimeToLiveUnitNanoseconds
	}
	if cu == "ms" {
		return options.TimeToLiveUnitMilliseconds
	}
	if cu == "us" {
		return options.TimeToLiveUnitMicroseconds
	}
	if cu == "s" {
		return options.TimeToLiveUnitSeconds
	}

	return options.TimeToLiveUnitUnspecified
}

func prepareCreateTableRequest(r *TableResource) (string, []options.CreateTableOption) {
	path := r.Path

	var opts []options.CreateTableOption
	for _, v := range r.Columns {
		opts = append(opts, options.WithColumn(v.Name, stringToYDBType(v.Type)))
	}

	for _, v := range r.Indexes {
		typ := options.GlobalIndex()
		if v.Type == "global_async" {
			typ = options.GlobalAsyncIndex()
		}
		opts = append(opts, options.WithIndex(v.Name, options.WithIndexColumns(v.Columns...), options.WithIndexType(typ)))
	}

	if r.TTL != nil {
	}

	opts = append(opts, options.WithPrimaryKeyColumn(r.PrimaryKey.Columns...))

	for k, v := range r.Attributes {
		opts = append(opts, options.WithAttribute(k, v))
	}

	var partitioningOpts []options.PartitioningSettingsOption
	if r.AutoPartitioning != nil {
		if r.AutoPartitioning.ByLoad != nil && *r.AutoPartitioning.ByLoad {
			partitioningOpts = append(partitioningOpts, options.WithPartitioningByLoad(options.FeatureEnabled))
		}
		if r.AutoPartitioning.BySize != nil && *r.AutoPartitioning.BySize > 0 {
			partitioningOpts = append(partitioningOpts, options.WithPartitioningBySize(options.FeatureEnabled))
			partitioningOpts = append(partitioningOpts, options.WithPartitionSizeMb(uint64(*r.AutoPartitioning.BySize)))
		}
	}

	var partitioningPolicyOpts []options.PartitioningPolicyOption
	if r.PartitioningPolicy != nil {
		if len(r.PartitioningPolicy.ExplicitPartitions) > 0 {
			parts := make([]types.Value, 0, len(r.PartitioningPolicy.ExplicitPartitions))
			for _, v := range r.PartitioningPolicy.ExplicitPartitions {
				parts = append(parts, types.Uint64Value(uint64(v)))
			}
			partitioningPolicyOpts = append(partitioningPolicyOpts, options.WithPartitioningPolicyExplicitPartitions(parts...))
		}
		if r.PartitioningPolicy.MaxPartitionsCount != 0 {
			partitioningOpts = append(partitioningOpts, options.WithMaxPartitionsCount(uint64(r.PartitioningPolicy.MaxPartitionsCount)))
		}
		if r.PartitioningPolicy.MinPartitionsCount != 0 {
			partitioningOpts = append(partitioningOpts, options.WithMinPartitionsCount(uint64(r.PartitioningPolicy.MinPartitionsCount)))
		}
		if r.PartitioningPolicy.PartitionsCount != 0 {
			partitioningPolicyOpts = append(partitioningPolicyOpts, options.WithPartitioningPolicyUniformPartitions(uint64(r.PartitioningPolicy.PartitionsCount)))
		}
	}

	if len(partitioningOpts) != 0 {
		opts = append(opts, options.WithPartitioningSettings(partitioningOpts...))
	}
	if len(partitioningPolicyOpts) != 0 {
		opts = append(opts, options.WithProfile(options.WithPartitioningPolicy(partitioningPolicyOpts...)))
	}

	if r.EnableBloomFilter != nil && *r.EnableBloomFilter {
		opts = append(opts, options.WithKeyBloomFilter(options.FeatureEnabled))
	}

	return path, opts
}

func TableCreate(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
	tableResource, err := tableResourceSchemaToTableResource(d)
	if err != nil {
		return diag.FromErr(err)
	}
	if tableResource == nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "got nil resource, unreachable code",
			},
		}
	}

	db, err := tbl.CreateDBConnection(ctx, tbl.TableClientParams{
		DatabaseEndpoint: tableResource.DatabaseEndpoint,
		Token:            tableResource.Token,
	})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "failed to initialize table client",
				Detail:   err.Error(),
			},
		}
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	tableSession, err := db.Table().CreateSession(ctx)
	if err != nil {
		return diag.Diagnostics{
			{
				Severity: diag.Error,
				Summary:  "failed to create table session",
				Detail:   err.Error(),
			},
		}
	}
	defer func() {
		_ = tableSession.Close(ctx)
	}()

	path, opts := prepareCreateTableRequest(tableResource)
	err = tableSession.CreateTable(ctx, path, opts...)
	if err != nil {
		return diag.Diagnostics{
			{
				Severity: diag.Error,
				Summary:  "failed to create table",
				Detail:   err.Error(),
			},
		}
	}

	d.SetId(tableResource.Path)

	return TableRead(ctx, d, cfg)
}
