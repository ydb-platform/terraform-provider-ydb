package helpers

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"google.golang.org/grpc"

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
