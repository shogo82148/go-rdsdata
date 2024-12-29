package rdsdata

import (
	"database/sql/driver"
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

	dialect     Dialect
	converters  []FieldConverter
	columnNames []string
}

func newRows(dialect Dialect, results []*rdsdata.ExecuteStatementOutput) *Rows {
	row := &Rows{
		results: results,
		dialect: dialect,
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
	curr := r.results[r.resultPosition]
	if r.recordPosition >= len(curr.Records) {
		return io.EOF
	}

	row := curr.Records[r.recordPosition]
	r.recordPosition++
	for i, field := range row {
		v, err := convertDefault(field)
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
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

	r.converters = make([]FieldConverter, len(curr.ColumnMetadata))
	r.columnNames = make([]string, len(curr.ColumnMetadata))
	for i, col := range curr.ColumnMetadata {
		r.converters[i] = r.dialect.GetFieldConverter(aws.ToString(col.TypeName))
		r.columnNames[i] = aws.ToString(col.Label)
	}
}
