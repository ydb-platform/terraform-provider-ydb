package externaldatasource

import (
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
)

type Resource struct {
	Entity           *helpers.YDBEntity
	FullPath         string
	Path             string
	DatabaseEndpoint string

	// Values holds Terraform string attributes (snake_case keys) for the EXTERNAL DATA SOURCE WITH clause.
	Values map[string]string
	UseTLS *bool
}

// allStringAttrKeys is every Terraform string attribute mirrored in Resource.Values.
// Slice order is only the stable iteration order for reads/writes and WITH emission (not a semantic contract).
var allStringAttrKeys = []string{
	"source_type", "location",
	"auth_method", "login", "password_secret_path",
	"service_account_id", "service_account_secret_path",
	"aws_access_key_id_secret_path",
	"aws_secret_access_key_secret_path",
	"aws_region",
	"token_secret_path",
	"database_name", "protocol", "mdb_cluster_id",
	"schema", "service_name", "folder_id",
	"grpc_location", "project", "cluster",
	"database_id",
	"reading_mode", "unexpected_type_display_mode", "unsupported_type_display_mode",
}

func (r *Resource) strAttr(key string) string {
	if r == nil || r.Values == nil {
		return ""
	}
	return r.Values[key]
}

func yqlStr(yqlUpper string) func(*Resource) string {
	tf := strings.ToLower(yqlUpper)
	return func(r *Resource) string { return r.strAttr(tf) }
}

func (r *Resource) getConnectionString() string {
	if r.DatabaseEndpoint != "" {
		return r.DatabaseEndpoint
	}
	return r.Entity.PrepareFullYDBEndpoint()
}

func resourceSchemaToResource(d *schema.ResourceData) (*Resource, error) {
	var entity *helpers.YDBEntity
	var err error
	if d.Id() != "" {
		entity, err = helpers.ParseYDBEntityID(d.Id())
		if err != nil {
			return nil, fmt.Errorf("failed to parse external data source entity: %w", err)
		}
	}

	databaseEndpoint := d.Get("connection_string").(string)
	var path string
	if entity != nil {
		path = entity.GetEntityPath()
		databaseEndpoint = entity.PrepareFullYDBEndpoint()
	} else {
		_, err := url.Parse(databaseEndpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database endpoint: %w", err)
		}
		path = d.Get("path").(string)
	}

	vals := make(map[string]string, len(allStringAttrKeys))
	for _, k := range allStringAttrKeys {
		vals[k] = d.Get(k).(string)
	}
	res := &Resource{
		Entity:           entity,
		FullPath:         path,
		Path:             helpers.TrimPath(d.Get("path").(string)),
		DatabaseEndpoint: databaseEndpoint,
		Values:           vals,
	}
	if v, ok := d.GetOk("use_tls"); ok {
		b := v.(bool)
		res.UseTLS = &b
	}
	return res, nil
}

type secretSpec struct {
	yqlKey    string // e.g. "PASSWORD_SECRET_PATH"
	getter    func(*Resource) string
	mandatory bool
}

// authMethodSpec describes one AUTH_METHOD: required plain fields, optional YQL keys, and secrets.
type authMethodSpec struct {
	mandatoryPlain map[string]func(*Resource) string
	optionalKeys   []string // YQL keys allowed but not required (see authYQLKeys)
	secrets        []secretSpec
}

var authSpecs = map[string]authMethodSpec{
	"NONE": {},
	"BASIC": {
		mandatoryPlain: map[string]func(*Resource) string{
			"LOGIN": yqlStr("LOGIN"),
		},
		secrets: []secretSpec{{yqlKey: "PASSWORD_SECRET_PATH", getter: yqlStr("PASSWORD_SECRET_PATH"), mandatory: true}},
	},
	"MDB_BASIC": {
		mandatoryPlain: map[string]func(*Resource) string{
			"SERVICE_ACCOUNT_ID": yqlStr("SERVICE_ACCOUNT_ID"),
			"LOGIN":              yqlStr("LOGIN"),
		},
		optionalKeys: []string{"MDB_CLUSTER_ID"},
		secrets: []secretSpec{
			{yqlKey: "SERVICE_ACCOUNT_SECRET_PATH", getter: yqlStr("SERVICE_ACCOUNT_SECRET_PATH"), mandatory: true},
			{yqlKey: "PASSWORD_SECRET_PATH", getter: yqlStr("PASSWORD_SECRET_PATH"), mandatory: true},
		},
	},
	"AWS": {
		mandatoryPlain: map[string]func(*Resource) string{
			"AWS_REGION": yqlStr("AWS_REGION"),
		},
		secrets: []secretSpec{
			{yqlKey: "AWS_ACCESS_KEY_ID_SECRET_PATH", getter: yqlStr("AWS_ACCESS_KEY_ID_SECRET_PATH"), mandatory: true},
			{yqlKey: "AWS_SECRET_ACCESS_KEY_SECRET_PATH", getter: yqlStr("AWS_SECRET_ACCESS_KEY_SECRET_PATH"), mandatory: true},
		},
	},
	"TOKEN": {
		secrets: []secretSpec{{yqlKey: "TOKEN_SECRET_PATH", getter: yqlStr("TOKEN_SECRET_PATH"), mandatory: true}},
	},
	"SERVICE_ACCOUNT": {
		mandatoryPlain: map[string]func(*Resource) string{
			"SERVICE_ACCOUNT_ID": yqlStr("SERVICE_ACCOUNT_ID"),
		},
		secrets: []secretSpec{{yqlKey: "SERVICE_ACCOUNT_SECRET_PATH", getter: yqlStr("SERVICE_ACCOUNT_SECRET_PATH"), mandatory: true}},
	},
}

// authYQLKeys lists every auth-related YQL property name (uppercase) checked by validateResourceAuth.
var authYQLKeys = []string{
	"LOGIN", "PASSWORD_SECRET_PATH",
	"SERVICE_ACCOUNT_ID", "SERVICE_ACCOUNT_SECRET_PATH",
	"AWS_ACCESS_KEY_ID_SECRET_PATH",
	"AWS_SECRET_ACCESS_KEY_SECRET_PATH",
	"AWS_REGION",
	"TOKEN_SECRET_PATH",
	"MDB_CLUSTER_ID",
}

func (s *authMethodSpec) allowedAuthFields(r *Resource, method string) (map[string]bool, error) {
	allowed := make(map[string]bool)
	for key, getter := range s.mandatoryPlain {
		allowed[key] = true
		if getter(r) == "" {
			return nil, fmt.Errorf("%s is required for AUTH_METHOD = %q", key, method)
		}
	}
	for _, key := range s.optionalKeys {
		allowed[key] = true
	}

	for _, sec := range s.secrets {
		allowed[sec.yqlKey] = true
		if sec.mandatory && sec.getter(r) == "" {
			return nil, fmt.Errorf("%s is required for AUTH_METHOD = %q", sec.yqlKey, method)
		}
	}
	return allowed, nil
}

// validateResourceAuth checks auth fields for the planned/configured resource (CustomizeDiff only).
func validateResourceAuth(r *Resource) error {
	method := r.strAttr("auth_method")
	if method == "" {
		for _, yqlKey := range authYQLKeys {
			if r.strAttr(strings.ToLower(yqlKey)) != "" {
				return fmt.Errorf("auth_method is required when login, secrets, or other auth-related attributes are set")
			}
		}
		return nil
	}

	spec, ok := authSpecs[method]
	if !ok {
		return fmt.Errorf("unknown AUTH_METHOD %q", method)
	}

	allowed, err := spec.allowedAuthFields(r, method)
	if err != nil {
		return err
	}
	for _, yqlKey := range authYQLKeys {
		if r.strAttr(strings.ToLower(yqlKey)) != "" && !allowed[yqlKey] {
			return fmt.Errorf("%s is not supported for AUTH_METHOD = %q", yqlKey, method)
		}
	}
	return nil
}

// sourceTypeAuthMethods maps each source type to its allowed AUTH_METHODs.
// Source: https://github.com/ydb-platform/ydb/blob/1612d5af9e6dc3e283778ba18523a90b50177805/ydb/core/external_sources/external_source_factory.cpp#L114-L181
var sourceTypeAuthMethods = map[string][]string{
	"ObjectStorage": {"NONE", "BASIC", "MDB_BASIC", "AWS", "TOKEN", "SERVICE_ACCOUNT"},
	"ClickHouse":    {"MDB_BASIC", "BASIC"},
	"PostgreSQL":    {"MDB_BASIC", "BASIC"},
	"MySQL":         {"MDB_BASIC", "BASIC"},
	"Ydb":           {"NONE", "BASIC", "SERVICE_ACCOUNT", "TOKEN"},
	"YT":            {"NONE", "TOKEN"},
	"Greenplum":     {"MDB_BASIC", "BASIC"},
	"MsSQLServer":   {"BASIC"},
	"Oracle":        {"BASIC"},
	"Logging":       {"SERVICE_ACCOUNT"},
	"Solomon":       {"NONE", "TOKEN", "SERVICE_ACCOUNT"},
	"Redis":         {"BASIC"},
	"Prometheus":    {"BASIC"},
	"MongoDB":       {"BASIC"},
	"OpenSearch":    {"BASIC"},
	"YdbTopics":     {"NONE", "BASIC", "TOKEN"},
}

// sourceTypeProperties maps each source type to its allowed non-auth properties.
// Source: https://github.com/ydb-platform/ydb/blob/1612d5af9e6dc3e283778ba18523a90b50177805/ydb/core/external_sources/external_source_factory.cpp#L114-L181
var sourceTypeProperties = map[string][]string{
	"ObjectStorage": {},
	"ClickHouse":    {"database_name", "protocol", "mdb_cluster_id", "use_tls"},
	"PostgreSQL":    {"database_name", "protocol", "mdb_cluster_id", "use_tls", "schema"},
	"MySQL":         {"database_name", "mdb_cluster_id", "use_tls"},
	"Ydb":           {"database_name", "use_tls", "database_id"},
	"YT":            {},
	"Greenplum":     {"database_name", "mdb_cluster_id", "use_tls", "schema"},
	"MsSQLServer":   {"database_name", "use_tls"},
	"Oracle":        {"database_name", "use_tls", "service_name"},
	"Logging":       {"folder_id"},
	"Solomon":       {"use_tls", "grpc_location", "project", "cluster"},
	"Redis":         {"database_name", "use_tls"},
	"Prometheus":    {"protocol", "use_tls"},
	"MongoDB":       {"database_name", "use_tls", "reading_mode", "unexpected_type_display_mode", "unsupported_type_display_mode"},
	"OpenSearch":    {"database_name", "use_tls"},
	"YdbTopics":     {"database_name", "use_tls"},
}

// allPropertyKeys lists every non-auth property attribute that can appear in sourceTypeProperties.
var allPropertyKeys = []string{
	"database_name", "protocol", "mdb_cluster_id", "use_tls",
	"schema", "service_name", "folder_id",
	"grpc_location", "project", "cluster",
	"database_id",
	"reading_mode", "unexpected_type_display_mode", "unsupported_type_display_mode",
}

// validateSourceType checks that auth_method and properties are valid for the given source_type.
func validateSourceType(r *Resource) error {
	srcType := r.strAttr("source_type")
	if srcType == "" {
		return nil
	}

	// Validate auth_method × source_type.
	method := r.strAttr("auth_method")
	if method != "" {
		allowed, ok := sourceTypeAuthMethods[srcType]
		if ok {
			found := slices.Contains(allowed, method)
			if !found {
				return fmt.Errorf("AUTH_METHOD %q is not supported for SOURCE_TYPE %q (allowed: %v)", method, srcType, allowed)
			}
		}
	}

	// Validate properties × source_type.
	props, ok := sourceTypeProperties[srcType]
	if !ok {
		return nil
	}
	allowedSet := make(map[string]bool, len(props))
	for _, p := range props {
		allowedSet[p] = true
	}
	for _, key := range allPropertyKeys {
		if key == "use_tls" {
			if r.UseTLS != nil && !allowedSet[key] {
				return fmt.Errorf("USE_TLS is not supported for SOURCE_TYPE %q", srcType)
			}
			continue
		}
		if r.strAttr(key) != "" && !allowedSet[key] {
			return fmt.Errorf("%s is not supported for SOURCE_TYPE %q", strings.ToUpper(key), srcType)
		}
	}
	return nil
}

func ydbUseTLSString(s string) bool {
	s = strings.TrimSpace(s)
	return strings.EqualFold(s, "TRUE") || s == "1"
}

func flattenDescription(d *schema.ResourceData, entity *helpers.YDBEntity, properties map[string]string, sourceType, location string) error {
	if err := d.Set("path", entity.GetEntityPath()); err != nil {
		return err
	}
	if err := d.Set("connection_string", entity.PrepareFullYDBEndpoint()); err != nil {
		return err
	}
	if err := d.Set("source_type", sourceType); err != nil {
		return err
	}
	if err := d.Set("location", location); err != nil {
		return err
	}

	for _, attr := range allStringAttrKeys {
		if attr == "source_type" || attr == "location" {
			continue
		}
		yqlKey := strings.ToUpper(attr)
		val, ok := properties[yqlKey]
		if !ok || val == "" {
			continue
		}
		if err := d.Set(attr, val); err != nil {
			return err
		}
	}
	if tlsVal, ok := properties["USE_TLS"]; ok {
		return d.Set("use_tls", ydbUseTLSString(tlsVal))
	}
	return nil
}
