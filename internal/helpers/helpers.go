package helpers

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type TerraformCRUD func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics

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
