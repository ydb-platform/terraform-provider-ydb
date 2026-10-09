package terraform_test

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestAccYdbTableIndex_globalUnique(t *testing.T) {
	conn := os.Getenv(envAccYDBConnection)
	tablePath := "tf_acc_unique/tbl_" + accRandomHex8(t)
	tableConfig := accTestConfigPrefix(conn) + fmt.Sprintf(`
resource "ydb_table" "test" {
  connection_string = var.connection_string
  path              = %q

  column {
    name = "id"
    type = "Uint64"
  }
  column {
    name = "email"
    type = "Utf8"
  }

  primary_key = ["id"]
}
`, tablePath)

	syncIndexConfig := tableConfig + `
resource "ydb_table_index" "unique_email" {
  table_id = ydb_table.test.id
  name     = "unique_email"
  type     = "global_sync"
  columns  = ["email"]
}
`
	defaultIndexConfig := tableConfig + `
resource "ydb_table_index" "unique_email" {
  table_id = ydb_table.test.id
  name     = "unique_email"
  type     = ""
  columns  = ["email"]
}
`
	defaultIndexWithoutTypeConfig := tableConfig + `
resource "ydb_table_index" "unique_email" {
  table_id = ydb_table.test.id
  name     = "unique_email"
  columns  = ["email"]
}
`

	indexConfig := tableConfig + `
resource "ydb_table_index" "unique_email" {
  table_id = ydb_table.test.id
  name     = "unique_email"
  type     = "global_unique_index"
  columns  = ["email"]
}
`
	invalidConfig := tableConfig + `
resource "ydb_table_index" "unique_email" {
  table_id = ydb_table.test.id
  name     = "unique_email"
  type     = "gobal_async"
  columns  = ["email"]
}
`

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { accPreCheckYDB(t) },
		ProviderFactories: accProviderFactories(),
		Steps: []resource.TestStep{
			{
				Config: tableConfig,
				Check:  checkInsertBeforeIndex(t, tablePath),
			},
			{
				Config: defaultIndexConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					checkNonUniqueIndexAllowsDuplicate(t, tablePath),
					checkIndexReadable(t, tablePath),
					resource.TestCheckResourceAttr("ydb_table_index.unique_email", "type", "global_sync"),
				),
			},
			{
				Config:   defaultIndexConfig,
				PlanOnly: true,
			},
			{
				Config:   defaultIndexWithoutTypeConfig,
				PlanOnly: true,
			},
			{
				Config: indexConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					checkUniqueIndexRejectsDuplicate(t, tablePath),
					checkIndexReadable(t, tablePath),
					resource.TestCheckResourceAttr("ydb_table_index.unique_email", "type", "global_unique_index"),
					resource.TestCheckResourceAttr("ydb_table_index.unique_email", "columns.0", "email"),
				),
			},
			{
				Config:   indexConfig,
				PlanOnly: true,
			},
			{
				Config: syncIndexConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					checkNonUniqueIndexAllowsDuplicate(t, tablePath),
					resource.TestCheckResourceAttr("ydb_table_index.unique_email", "type", "global_sync"),
				),
			},
			{
				Config:   syncIndexConfig,
				PlanOnly: true,
			},
			{
				Config: indexConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					checkUniqueIndexRejectsDuplicate(t, tablePath),
					resource.TestCheckResourceAttr("ydb_table_index.unique_email", "type", "global_unique_index"),
				),
			},
			{
				Config:   indexConfig,
				PlanOnly: true,
			},
			{
				Config:      invalidConfig,
				PlanOnly:    true,
				ExpectError: regexp.MustCompile(`expected type to be one of`),
			},
			{
				Config: tableConfig,
				Check:  checkUniqueIndexRemoved(t, tablePath),
			},
		},
	})
}

func checkInsertBeforeIndex(t *testing.T, tablePath string) resource.TestCheckFunc {
	t.Helper()
	return func(_ *terraform.State) error {
		db := accOpenYDB(t)
		query := fmt.Sprintf("INSERT INTO `%s` (id, email) VALUES (1u, 'same@example.com')", tablePath)
		if err := db.Query().Exec(t.Context(), query); err != nil {
			return fmt.Errorf("insert row before adding unique index: %w", err)
		}
		return nil
	}
}

func checkUniqueIndexRejectsDuplicate(t *testing.T, tablePath string) resource.TestCheckFunc {
	t.Helper()
	return func(_ *terraform.State) error {
		db := accOpenYDB(t)
		query := fmt.Sprintf("INSERT INTO `%s` (id, email) VALUES (2u, 'same@example.com')", tablePath)
		if err := db.Query().Exec(t.Context(), query); err == nil || !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
			return fmt.Errorf("duplicate indexed value: want PRECONDITION_FAILED, got %w", err)
		}
		row, err := db.Query().QueryRow(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tablePath))
		if err != nil {
			return fmt.Errorf("count rows after rejected insert: %w", err)
		}
		var count uint64
		if err := row.Scan(&count); err != nil {
			return fmt.Errorf("scan row count: %w", err)
		}
		if count != 1 {
			return fmt.Errorf("rows after rejected insert: got %d, want 1", count)
		}
		return nil
	}
}

func checkNonUniqueIndexAllowsDuplicate(t *testing.T, tablePath string) resource.TestCheckFunc {
	t.Helper()
	return func(_ *terraform.State) error {
		db := accOpenYDB(t)
		query := fmt.Sprintf("INSERT INTO `%s` (id, email) VALUES (2u, 'same@example.com')", tablePath)
		if err := db.Query().Exec(t.Context(), query); err != nil {
			return fmt.Errorf("insert duplicate with non-unique index: %w", err)
		}
		row, err := db.Query().QueryRow(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tablePath))
		if err != nil {
			return fmt.Errorf("count rows with non-unique index: %w", err)
		}
		var count uint64
		if err := row.Scan(&count); err != nil {
			return fmt.Errorf("scan row count with non-unique index: %w", err)
		}
		if count != 2 {
			return fmt.Errorf("rows with non-unique index: got %d, want 2", count)
		}
		if err := db.Query().Exec(t.Context(), fmt.Sprintf("DELETE FROM `%s` WHERE id = 2u", tablePath)); err != nil {
			return fmt.Errorf("remove duplicate before enabling unique index: %w", err)
		}
		return nil
	}
}

func checkUniqueIndexRemoved(t *testing.T, tablePath string) resource.TestCheckFunc {
	t.Helper()
	return func(_ *terraform.State) error {
		db := accOpenYDB(t)
		row, err := db.Query().QueryRow(t.Context(), uniqueIndexLookupQuery(tablePath))
		if err == nil {
			var id uint64
			err = row.Scan(&id)
		}
		if !ydb.IsOperationErrorSchemeError(err) {
			return fmt.Errorf("query through removed unique index: want SCHEME_ERROR, got %w", err)
		}

		query := fmt.Sprintf("INSERT INTO `%s` (id, email) VALUES (2u, 'same@example.com')", tablePath)
		if err := db.Query().Exec(t.Context(), query); err != nil {
			return fmt.Errorf("insert duplicate value after removing unique index: %w", err)
		}
		row, err = db.Query().QueryRow(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tablePath))
		if err != nil {
			return fmt.Errorf("count rows after removing unique index: %w", err)
		}
		var count uint64
		if err := row.Scan(&count); err != nil {
			return fmt.Errorf("scan row count after removing unique index: %w", err)
		}
		if count != 2 {
			return fmt.Errorf("rows after removing unique index: got %d, want 2", count)
		}
		return nil
	}
}

func checkIndexReadable(t *testing.T, tablePath string) resource.TestCheckFunc {
	t.Helper()
	return func(_ *terraform.State) error {
		db := accOpenYDB(t)
		row, err := db.Query().QueryRow(t.Context(), uniqueIndexLookupQuery(tablePath))
		if err != nil {
			return fmt.Errorf("query through unique index: %w", err)
		}
		var id uint64
		if err := row.Scan(&id); err != nil {
			return fmt.Errorf("scan row through unique index: %w", err)
		}
		if id != 1 {
			return fmt.Errorf("row through unique index: got id %d, want 1", id)
		}
		return nil
	}
}

func uniqueIndexLookupQuery(tablePath string) string {
	return fmt.Sprintf("SELECT id FROM `%s` VIEW unique_email WHERE email = 'same@example.com'", tablePath)
}
