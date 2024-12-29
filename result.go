package rdsdata

import (
	"database/sql/driver"

	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

// compile time type check
var _ driver.Result = (*Result)(nil)

// newResult creates a new result.
func newResult(results []*rdsdata.ExecuteStatementOutput) *Result {
	var rowsAffected int64
	var lastInsertID int64
	for _, result := range results {
		rowsAffected += result.NumberOfRecordsUpdated
		if len(result.GeneratedFields) == 1 {
			field := result.GeneratedFields[0]
			if fv, ok := field.(*types.FieldMemberLongValue); ok {
				lastInsertID = fv.Value
			}
		}
	}
	return &Result{
		rowsAffected: rowsAffected,
		lastInsertID: lastInsertID,
	}
}

// Result is the result of a query.
type Result struct {
	rowsAffected int64
	lastInsertID int64
}

// RowsAffected returns the number of rows affected.
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// LastInsertId returns the last inserted ID.
func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}
