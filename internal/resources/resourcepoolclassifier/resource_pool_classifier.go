package resourcepoolclassifier

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	"github.com/ydb-platform/terraform-provider-ydb/internal/helpers"
	tbl "github.com/ydb-platform/terraform-provider-ydb/internal/table"
)

type resourcePoolClassifier struct {
	Name         string
	Rank         int
	ResourcePool string
	MemberName   string
	HasRank      bool
	HasMember    bool
}

func resourcePoolClassifierFromData(d *schema.ResourceData) resourcePoolClassifier {
	rank, hasRank := d.GetOk("rank")
	memberName, hasMember := d.GetOk("member_name")
	classifier := resourcePoolClassifier{
		Name:         d.Get("name").(string),
		ResourcePool: d.Get("resource_pool").(string),
		HasRank:      hasRank,
		HasMember:    hasMember,
	}
	if hasRank {
		classifier.Rank = rank.(int)
	}
	if hasMember {
		classifier.MemberName = memberName.(string)
	}
	return classifier
}

func buildCreateQuery(classifier resourcePoolClassifier) string {
	settings := []string{
		"RESOURCE_POOL = " + quoteString(classifier.ResourcePool),
	}
	if classifier.HasRank {
		settings = append(settings, fmt.Sprintf("RANK = %d", classifier.Rank))
	}
	if classifier.HasMember {
		settings = append(settings, "MEMBER_NAME = "+quoteString(classifier.MemberName))
	}

	return fmt.Sprintf(
		"CREATE RESOURCE POOL CLASSIFIER %s WITH (\n    %s\n)",
		quoteIdentifier(classifier.Name),
		strings.Join(settings, ",\n    "),
	)
}

func buildAlterQuery(classifier resourcePoolClassifier) string {
	settings := []string{
		fmt.Sprintf("RANK = %d", classifier.Rank),
		"RESOURCE_POOL = " + quoteString(classifier.ResourcePool),
	}
	if classifier.HasMember {
		settings = append(settings, "MEMBER_NAME = "+quoteString(classifier.MemberName))
	}
	queryText := fmt.Sprintf(
		"ALTER RESOURCE POOL CLASSIFIER %s SET (\n    %s\n)",
		quoteIdentifier(classifier.Name),
		strings.Join(settings, ",\n    "),
	)
	if !classifier.HasMember {
		queryText += ", RESET (MEMBER_NAME)"
	}
	return queryText
}

func quoteIdentifier(name string) string {
	return "`" + helpers.EscapeYQLIdentifier(name) + "`"
}

func quoteString(value string) string {
	return "'" + helpers.EscapeYQLString(value) + "'"
}

func (h *handler) Create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	connectionString := d.Get("connection_string").(string)
	classifier := resourcePoolClassifierFromData(d)

	db, err := h.openDB(ctx, connectionString)
	if err != nil {
		return diag.FromErr(err)
	}
	defer func() { _ = db.Close(ctx) }()

	if err = db.Query().Exec(ctx, buildCreateQuery(classifier)); err != nil {
		return diag.Errorf("failed to execute CREATE RESOURCE POOL CLASSIFIER for %q: %s", classifier.Name, err)
	}

	d.SetId(connectionString + "?path=" + classifier.Name)
	return h.Read(ctx, d, meta)
}

func (h *handler) Read(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	db, err := h.openDB(ctx, entity.PrepareFullYDBEndpoint())
	if err != nil {
		return diag.FromErr(err)
	}
	defer func() { _ = db.Close(ctx) }()

	var (
		name         string
		rank         int64
		resourcePool string
		memberName   string
	)
	row, err := db.Query().QueryRow(ctx, `
SELECT
    Name,
    Rank,
    ResourcePool,
    COALESCE(MemberName, "") AS MemberName
FROM `+"`"+`.sys/resource_pool_classifiers`+"`"+`
WHERE Name = $name;
`,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$name").Text(entity.GetEntityPath()).
				Build(),
		),
		query.WithIdempotent(),
	)
	if errors.Is(err, query.ErrNoRows) {
		d.SetId("")
		return nil
	}
	if err != nil {
		return diag.Errorf(
			"failed to read resource pool classifier %q from .sys/resource_pool_classifiers: %s",
			entity.GetEntityPath(),
			err,
		)
	}

	err = row.ScanNamed(
		query.Named("Name", &name),
		query.Named("Rank", &rank),
		query.Named("ResourcePool", &resourcePool),
		query.Named("MemberName", &memberName),
	)
	if err != nil {
		return diag.Errorf(
			"failed to scan resource pool classifier %q from .sys/resource_pool_classifiers: %s",
			entity.GetEntityPath(),
			err,
		)
	}

	if err = setResourcePoolClassifierState(
		d,
		entity.PrepareFullYDBEndpoint(),
		name,
		int(rank),
		resourcePool,
		memberName,
	); err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func (h *handler) Update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	classifier := resourcePoolClassifierFromData(d)
	classifier.Name = entity.GetEntityPath()
	classifier.HasRank = true

	db, err := h.openDB(ctx, entity.PrepareFullYDBEndpoint())
	if err != nil {
		return diag.FromErr(err)
	}
	defer func() { _ = db.Close(ctx) }()

	if err = db.Query().Exec(ctx, buildAlterQuery(classifier)); err != nil {
		return diag.Errorf("failed to execute ALTER RESOURCE POOL CLASSIFIER for %q: %s", classifier.Name, err)
	}

	return h.Read(ctx, d, meta)
}

func (h *handler) Delete(ctx context.Context, d *schema.ResourceData, _ interface{}) diag.Diagnostics {
	entity, err := helpers.ParseYDBEntityID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	db, err := h.openDB(ctx, entity.PrepareFullYDBEndpoint())
	if err != nil {
		return diag.FromErr(err)
	}
	defer func() { _ = db.Close(ctx) }()

	err = db.Query().Exec(ctx, "DROP RESOURCE POOL CLASSIFIER "+quoteIdentifier(entity.GetEntityPath()))
	if err != nil && !ydb.IsOperationErrorSchemeError(err) && !ydb.IsOperationErrorNotFoundError(err) {
		return diag.Errorf("failed to execute DROP RESOURCE POOL CLASSIFIER for %q: %s", entity.GetEntityPath(), err)
	}

	d.SetId("")
	return nil
}

func (h *handler) openDB(ctx context.Context, connectionString string) (*ydb.Driver, error) {
	db, err := tbl.CreateDBConnection(ctx, tbl.ClientParams{
		DatabaseEndpoint: connectionString,
		AuthCreds:        h.authCreds,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize YDB client: %w", err)
	}
	return db, nil
}

func setResourcePoolClassifierState(
	d *schema.ResourceData,
	connectionString string,
	name string,
	rank int,
	resourcePool string,
	memberName string,
) error {
	values := map[string]interface{}{
		"connection_string": connectionString,
		"name":              name,
		"rank":              rank,
		"resource_pool":     resourcePool,
		"member_name":       memberName,
	}
	for key, value := range values {
		if err := d.Set(key, value); err != nil {
			return fmt.Errorf("failed to set resource pool classifier attribute %q: %w", key, err)
		}
	}
	return nil
}
