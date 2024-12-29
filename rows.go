package rdsdata

import (
	"database/sql/driver"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
)

// compile time type check
var _ driver.Rows = (*Rows)(nil)
var _ driver.RowsNextResultSet = (*Rows)(nil)

type Rows struct {
	results        []*rdsdata.ExecuteStatementOutput
	resultPosition int
	recordPosition int

	columnNames []string
}

func newRows(results []*rdsdata.ExecuteStatementOutput) *Rows {
	row := &Rows{
		results: results,
	}
	row.setResultIndex(0)
	return row
}

// Columns returns the columns.
func (r *Rows) Columns() []string {
	return r.columnNames
}

// Close closes the rows.
func (r *Rows) Close() error {
	return nil
}

// Next moves to the next row.
func (r *Rows) Next(dest []driver.Value) error {
	return errors.New("not implemented")
}

// NextResultSet moves to the next result set.
func (r *Rows) HasNextResultSet() bool {
	return r.resultPosition+1 < len(r.results)
}

// NextResultSet moves to the next result set.
func (r *Rows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}
	r.setResultIndex(r.resultPosition + 1)
	return nil
}

func (r *Rows) setResultIndex(index int) {
	r.resultPosition = index
	r.recordPosition = 0
	curr := r.results[r.resultPosition]

	r.columnNames = make([]string, len(curr.ColumnMetadata))
	for i, col := range curr.ColumnMetadata {
		r.columnNames[i] = aws.ToString(col.Name)
	}
}
