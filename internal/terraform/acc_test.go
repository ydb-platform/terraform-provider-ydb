package terraform_test

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	ydbprovider "github.com/ydb-platform/terraform-provider-ydb/ydb"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

// Shared acceptance-test setup for this package. Tests require Terraform CLI on PATH.
//
//	YDB_ACC_CONNECTION_STRING=grpc://127.0.0.1:2136/?database=/local TF_ACC=1 go test -v ./internal/terraform/ -run TestAcc -timeout 30m
//
// Optional provider auth: YDB_ACC_TOKEN, YDB_ACC_USER, YDB_ACC_PASSWORD.

const envAccYDBConnection = "YDB_ACC_CONNECTION_STRING"

func accProviderBlock() string {
	var b strings.Builder
	b.WriteString(`provider "ydb" {`)
	if v := os.Getenv("YDB_ACC_TOKEN"); v != "" {
		fmt.Fprintf(&b, "\n  token = %q", v)
	}
	if u := os.Getenv("YDB_ACC_USER"); u != "" {
		fmt.Fprintf(&b, "\n  user = %q", u)
	}
	if p := os.Getenv("YDB_ACC_PASSWORD"); p != "" {
		fmt.Fprintf(&b, "\n  password = %q", p)
	}
	b.WriteString("\n}\n")
	return b.String()
}

func accPreCheckYDB(t *testing.T) {
	t.Helper()
	if os.Getenv(envAccYDBConnection) == "" {
		t.Fatalf("%s must be set for acceptance tests", envAccYDBConnection)
	}
}

func accProviderFactories() map[string]func() (*schema.Provider, error) {
	return map[string]func() (*schema.Provider, error){
		"ydb": func() (*schema.Provider, error) {
			return ydbprovider.Provider(), nil
		},
	}
}

func accRandomHex8(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// accYDBCatalogAbsPath returns the full YDB catalog path under the database from conn, using
// helpers.JoinYDBCatalogPath. If conn is empty or cannot be parsed, database root "/local" is used
// (local-ydb default for skipped acceptance runs).
func accYDBCatalogAbsPath(conn, pathUnderDB string) string {
	db := "/local"
	if conn != "" {
		if _, d, _, err := helpers.ParseYDBDatabaseEndpoint(conn); err == nil && d != "" {
			db = d
		}
	}
	return helpers.JoinYDBCatalogPath(db, pathUnderDB)
}

// accLocationHostPortFromConn returns "host:port" from a YDB grpc(s) connection string for use as
// external data source LOCATION (e.g. Ydb source type). Empty or invalid conn yields "localhost:2136".
func accLocationHostPortFromConn(conn string) string {
	if conn == "" {
		return "localhost:2136"
	}
	scheme := "grpc://"
	rep := "http://"
	if strings.HasPrefix(conn, "grpcs://") {
		scheme = "grpcs://"
		rep = "https://"
	} else if !strings.HasPrefix(conn, "grpc://") {
		return "localhost:2136"
	}
	u, err := url.Parse(strings.Replace(conn, scheme, rep, 1))
	if err != nil || u.Host == "" {
		return "localhost:2136"
	}
	return u.Host
}
