package rdsdata

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

func TestNewResult(t *testing.T) {
	t.Run("rowsAffected", func(t *testing.T) {
		results := []*rdsdata.ExecuteStatementOutput{
			{
				NumberOfRecordsUpdated: 1,
			},
			{
				NumberOfRecordsUpdated: 2,
			},
			{
				NumberOfRecordsUpdated: 3,
			},
		}
		result := newResult(results)

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if rowsAffected != 6 {
			t.Errorf("unexpected rowsAffected: %d, want 6", rowsAffected)
		}

		lastInsetID, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if lastInsetID != 0 {
			t.Errorf("unexpected lastInsertID: %d, want 0", lastInsetID)
		}
	})

	t.Run("lastInsertID", func(t *testing.T) {
		results := []*rdsdata.ExecuteStatementOutput{
			{
				GeneratedFields: []types.Field{
					&types.FieldMemberLongValue{
						Value: 42,
					},
				},
			},
		}
		result := newResult(results)

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if rowsAffected != 0 {
			t.Errorf("unexpected rowsAffected: %d, want 0", rowsAffected)
		}

		lastInsetID, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if lastInsetID != 42 {
			t.Errorf("unexpected lastInsertID: %d, want 42", lastInsetID)
		}
	})
}
