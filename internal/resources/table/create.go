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
		return types.TypeUnknown
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
		mode := options.TimeToLiveModeDateType
		if r.TTL.Mode == "since_unix_epoch" {
			mode = options.TimeToLiveModeValueSinceUnixEpoch
		}
		var columnUnit *options.TimeToLiveUnit
		if mode == options.TimeToLiveModeValueSinceUnixEpoch {
			md := strColumnUnitToYDBColumnUnit(r.TTL.ColumnUnit)
			columnUnit = &md
		}
		opts = append(opts, options.WithTimeToLiveSettings(options.TimeToLiveSettings{
			ColumnName:         r.TTL.ColumnName,
			Mode:               mode,
			ExpireAfterSeconds: uint32(r.TTL.ExpireAfter.Seconds()),
			ColumnUnit:         columnUnit,
		}))
	}

	for k, v := range r.Attributes {
		opts = append(opts, options.WithAttribute(k, v))
	}

	return path, opts
}

func TableCreate(ctx context.Context, d *schema.ResourceData, cfg interface{}) diag.Diagnostics {
	tableResource := tableResourceSchemaToTableResource(d)
	if tableResource == nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "got nil resource, unreachable code",
			},
		}
	}

	client, err := tbl.CreateTableClient(ctx, tbl.TableClientParams{
		DatabaseEndpoint: tableResource.DatabaseEndpoint,
		Token:            "",
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
		_ = client.Close(ctx)
	}()

	tableSession, err := client.CreateSession(ctx)
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

	return nil
}
