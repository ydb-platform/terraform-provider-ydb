package helpers

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"google.golang.org/grpc"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers/topic"
	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

type TerraformCRUD func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics

var (
	listOfValidUnit = []string{"seconds", "milliseconds", "microseconds", "nanoseconds"}
	mapTTLUnit      = map[string]string{
		"UNIT_SECONDS": "seconds", "UNIT_MILLISECONDS": "milliseconds",
		"UNIT_MICROSECONDS": "microseconds", "UNIT_NANOSECONDS": "nanoseconds",
	}
)

func ParseYDBDatabaseEndpoint(endpoint string) (baseEP, databasePath string, useTLS bool, err error) {
	dbSplit := strings.Split(endpoint, "/?database=")
	if len(dbSplit) != 2 {
		return "", "", false, fmt.Errorf("cannot parse endpoint %q", endpoint)
	}
	parts := strings.SplitN(dbSplit[0], "/", 3)
	if len(parts) < 3 {
		return "", "", false, fmt.Errorf("cannot parse endpoint schema %q", dbSplit[0])
	}

	const (
		protocolGRPCS = "grpcs:"
		protocolGRPC  = "grpc:"
	)

	switch protocol := parts[0]; protocol {
	case protocolGRPCS:
		useTLS = true
	case protocolGRPC:
		useTLS = false
	default:
		return "", "", false, fmt.Errorf("unknown protocol %q", protocol)
	}
	return parts[2], dbSplit[1], useTLS, nil
}

func AppendWithEscape(buf []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		if s[i] == '"' || s[i] == '/' {
			buf = append(buf, '\\')
		}
		buf = append(buf, s[i])
	}
	return buf
}

func YdbTTLUnitCheck(i interface{}, k string) (warnings []string, errors []error) {
	v, ok := i.(string)
	if !ok {
		errors = append(errors, fmt.Errorf("expected type of %q to be string", k))
		return warnings, errors
	}

	for _, val := range listOfValidUnit {
		if val == v {
			return
		}
	}

	errors = append(errors, fmt.Errorf("valid value for %q not found, expected: %v", k, listOfValidUnit))

	return
}

func YdbTablePathCheck(i interface{}, k string) (warnings []string, errors []error) {
	v, ok := i.(string)
	if !ok {
		errors = append(errors, fmt.Errorf("expected type of %q to be string", k))
		return warnings, errors
	}

	if !strings.HasPrefix(v, "/") && !strings.HasSuffix(v, "/") {
		return
	}

	errors = append(errors, fmt.Errorf("table path %q can't start or end with '/'", v))

	return
}

func YDBUnitToUnit(unit string) string {
	return mapTTLUnit[unit]
}

func TrimPath(path string) string {
	return strings.Trim(path, "/")
}

func GetToken(ctx context.Context, creds auth.YdbCredentials, conn *grpc.ClientConn) (string, error) {
	if creds.User != "" {
		token, err := auth.GetTokenFromStaticCreds(ctx, creds.User, creds.Password, conn)
		if err != nil {
			return "", err
		}
		return token, nil
	}
	return creds.Token, nil
}

func ConsumerSort(schRaw interface{}, descRaw []topictypes.Consumer) []topictypes.Consumer {
	nameMap := make(map[string]topictypes.Consumer, len(descRaw))
	for _, c := range descRaw {
		nameMap[c.Name] = c
	}

	result := make([]topictypes.Consumer, 0, len(descRaw))

	for _, raw := range schRaw.([]interface{}) {
		schCons := raw.(map[string]interface{})
		name := schCons["name"].(string)

		if consumer, ok := nameMap[name]; ok {
			codecsRaw := schCons["supported_codecs"].([]interface{})
			supported := make(map[topictypes.Codec]struct{}, len(consumer.SupportedCodecs))
			for _, c := range consumer.SupportedCodecs {
				supported[c] = struct{}{}
			}

			var supHead, supTail []topictypes.Codec
			supHead = make([]topictypes.Codec, 0, len(codecsRaw))
			supTail = make([]topictypes.Codec, 0, len(codecsRaw))

			for _, cr := range codecsRaw {
				codecName := strings.ToLower(cr.(string))
				codec := topic.YDBTopicCodecNameToCodec[codecName]

				if _, ok := supported[codec]; ok {
					supHead = append(supHead, codec)
				} else {
					supTail = append(supTail, codec)
				}
			}

			supHead = append(supHead, supTail...)
			consumer.SupportedCodecs = supHead
			result = append(result, consumer)
			delete(nameMap, name)
		}
	}

	for _, c := range nameMap {
		result = append(result, c)
	}

	return result
}

func AreAllElementsUnique(consumers []topictypes.Consumer) error {
	// Используем struct{} вместо bool для экономии памяти
	uniqueConsumers := make(map[string]struct{}, len(consumers))
	var codecCache map[topictypes.Codec]struct{} // Будем переиспользовать мапу

	for _, consumer := range consumers {
		// Проверка уникальности имени потребителя
		if _, exists := uniqueConsumers[consumer.Name]; exists {
			return fmt.Errorf("non unique consumer: %s", consumer.Name)
		}
		uniqueConsumers[consumer.Name] = struct{}{}

		// Переиспользуем мапу с очисткой вместо создания новой
		if codecCache == nil {
			codecCache = make(map[topictypes.Codec]struct{}, len(consumer.SupportedCodecs))
		} else {
			clear(codecCache)
		}

		// Проверка уникальности кодеков
		for _, codec := range consumer.SupportedCodecs {
			if _, exists := codecCache[codec]; exists {
				codecName := topic.YDBTopicCodecToCodecName[codec] // Выносим преобразование
				return fmt.Errorf("non unique codec: %s in consumer: %s", codecName, consumer.Name)
			}
			codecCache[codec] = struct{}{}
		}
	}
	return nil
}
