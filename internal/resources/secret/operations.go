package secret

import (
	"context"
	"fmt"
	"strconv"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

// CreateSecret creates a secret with the supplied value and permission policy.
func CreateSecret(ctx context.Context, db *ydb.Driver, path, value string, inheritPermissions bool) error {
	escapedPath := helpers.EscapeYQLIdentifier(path)
	escapedValue := strconv.Quote(value)
	q := fmt.Sprintf("CREATE SECRET `%s` WITH (value = %s)", escapedPath, escapedValue)
	if inheritPermissions {
		q = fmt.Sprintf("CREATE SECRET `%s` WITH (value = %s, inherit_permissions = True)", escapedPath, escapedValue)
	}
	return db.Query().Exec(ctx, q)
}

// DropSecret removes the secret at path.
func DropSecret(ctx context.Context, db *ydb.Driver, path string) error {
	return db.Query().Exec(ctx, fmt.Sprintf("DROP SECRET `%s`", helpers.EscapeYQLIdentifier(path)))
}
