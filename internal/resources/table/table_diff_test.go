package table

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func TestCheckColumnDiff(t *testing.T) {
	testData := []struct {
		testName             string
		rcolumns             []*Column
		dcolumns             []options.Column
		expectedColumnsToAdd []*Column
		expectedError        bool
	}{
		{
			testName: "empty resource columns and empty table columns",
		},
		{
			testName: "non-empty resource columns and empty table columns",
			rcolumns: []*Column{
				{
					Name: "a",
				},
			},
			expectedColumnsToAdd: []*Column{
				{
					Name: "a",
				},
			},
		},
		{
			testName: "resource with deleting columns",
			rcolumns: []*Column{
				{
					Name: "a",
				},
				{
					Name: "b",
				},
			},
			dcolumns: []options.Column{
				{
					Name: "a",
				},
				{
					Name: "b",
				},
				{
					Name: "c",
				},
			},
			expectedError: true,
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got, err := checkColumnDiff(v.rcolumns, v.dcolumns)
			if v.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, v.expectedColumnsToAdd, got)
		})
	}
}

func TestCompareIndexes(t *testing.T) {
	testData := []struct {
		testName string
		ridx     Index
		didx     options.IndexDescription
		expected bool
	}{
		{
			testName: "equal indexes",
			ridx: Index{
				Name: "myidx",
				Columns: []string{
					"a", "b",
				},
				Cover: []string{
					"c", "d",
				},
			},
			didx: options.IndexDescription{
				Name: "myidx",
				IndexColumns: []string{
					"a", "b",
				},
				DataColumns: []string{
					"c", "d",
				},
			},
			expected: true,
		},
		{
			testName: "resource index without cover columns",
			ridx: Index{
				Name: "myidx",
				Columns: []string{
					"a", "b",
				},
			},
			didx: options.IndexDescription{
				Name: "myidx",
				IndexColumns: []string{
					"a", "b",
				},
				DataColumns: []string{
					"c", "d",
				},
			},
			expected: false,
		},
		{
			testName: "resource index with different cover columns",
			ridx: Index{
				Name: "myidx",
				Columns: []string{
					"a", "b",
				},
				Cover: []string{
					"c", "d",
				},
			},
			didx: options.IndexDescription{
				Name: "myidx",
				IndexColumns: []string{
					"a", "b",
				},
				DataColumns: []string{
					"e", "f",
				},
			},
			expected: false,
		},
		{
			testName: "resource index with less cover columns",
			ridx: Index{
				Name: "myidx",
				Columns: []string{
					"a", "b",
				},
				Cover: []string{
					"c",
				},
			},
			didx: options.IndexDescription{
				Name: "myidx",
				IndexColumns: []string{
					"a", "b",
				},
				DataColumns: []string{
					"c", "d",
				},
			},
			expected: false,
		},
		{
			testName: "resource index with different index columns",
			ridx: Index{
				Name: "myidx",
				Columns: []string{
					"b", "a", // 'b', 'a' instead of 'a', 'b'
				},
				Cover: []string{
					"c", "d",
				},
			},
			didx: options.IndexDescription{
				Name: "myidx",
				IndexColumns: []string{
					"a", "b",
				},
				DataColumns: []string{
					"c", "d",
				},
			},
			expected: false,
		},
		{
			testName: "resource index with less index columns",
			ridx: Index{
				Name: "myidx",
				Columns: []string{
					"a",
				},
				Cover: []string{
					"c", "d",
				},
			},
			didx: options.IndexDescription{
				Name: "myidx",
				IndexColumns: []string{
					"a", "b",
				},
				DataColumns: []string{
					"c", "d",
				},
			},
			expected: false,
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := compareIndexes(&v.ridx, v.didx)
			assert.Equal(t, v.expected, got)
		})
	}
}
