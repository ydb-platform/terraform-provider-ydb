package table

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type tableDiff struct {
	ColumnsToAdd            []*Column
	IndexToDrop             []*Index
	IndexToCreate           []*Index
	TTL                     *TTL
	NewPartitioningSettings *PartitioningSettings
	AddChangeFeed           bool
	DropChangeFeed          bool
}

func checkColumnDiff(rcolumns []*Column, dcolumns []options.Column) ([]*Column, error) {
	existingColumns := make(map[string]struct{})
	for _, v := range dcolumns {
		existingColumns[v.Name] = struct{}{}
	}
	resourceColumns := make(map[string]*Column)
	for _, v := range rcolumns {
		resourceColumns[v.Name] = v
	}

	var columnsToAdd []*Column
	var deletedColumns []string

	for k := range existingColumns {
		if _, ok := resourceColumns[k]; !ok {
			deletedColumns = append(deletedColumns, k)
		}
	}

	for k, v := range resourceColumns {
		if _, ok := existingColumns[k]; !ok {
			columnsToAdd = append(columnsToAdd, v)
		}
	}

	if len(deletedColumns) > 0 {
		return nil, fmt.Errorf("it is prohibited to delete columns with terraform. Columns for deletion: [%s]", strings.Join(deletedColumns, ","))
	}
	return columnsToAdd, nil
}

func compareIndexes(ridx *Index, didx options.IndexDescription) bool {
	if ridx.Name != didx.Name {
		return false
	}
	if len(ridx.Columns) != len(didx.IndexColumns) {
		return false
	}
	// TODO(shmel1k@): check index type, wait for go-sdk fix.
	for i := 0; i < len(ridx.Columns); i++ {
		if ridx.Columns[i] != didx.IndexColumns[i] {
			return false
		}
	}
	if len(ridx.Cover) != len(didx.DataColumns) {
		return false
	}
	mp1 := make(map[string]struct{})
	for _, v := range ridx.Cover {
		mp1[v] = struct{}{}
	}
	mp2 := make(map[string]struct{})
	for _, v := range didx.DataColumns {
		mp2[v] = struct{}{}
	}

	return reflect.DeepEqual(mp1, mp2)
}

func checkIndexDiff(rindexes []*Index, dindexes []options.IndexDescription) (toDrop []string, toAdd []*Index) {
	existingIndexes := make(map[string]struct{})
	for _, v := range dindexes {
		existingIndexes[v.Name] = struct{}{}
	}

	resourceIndexes := make(map[string]*Index)
	for _, v := range rindexes {
		resourceIndexes[v.Name] = v
	}

	return
}

func prepareTableDiff(d *schema.ResourceData, desc options.Description) (*tableDiff, error) {
	diff := &tableDiff{}
	if d.HasChange("column") {
		rColumns := expandColumns(d)
		newColumns, err := checkColumnDiff(rColumns, desc.Columns)
		if err != nil {
			return nil, err
		}
		diff.ColumnsToAdd = newColumns
	}

	return diff, nil
}
