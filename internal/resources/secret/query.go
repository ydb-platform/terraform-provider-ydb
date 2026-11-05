package secret

import (
	"strconv"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

// PrepareCreateQuery builds CREATE SECRET with an escaped String literal.
func PrepareCreateQuery(name, value string, inheritPermissions bool) string {
	q := "CREATE SECRET `" + helpers.EscapeYQLIdentifier(name) + "` WITH (value = " + strconv.Quote(value)
	if inheritPermissions {
		q += ", inherit_permissions = True"
	}
	return q + ")"
}
