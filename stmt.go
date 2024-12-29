package rdsdata

import (
	"context"
	"database/sql/driver"

	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
)

// compile time type check
var _ driver.Stmt = (*Stmt)(nil)
var _ driver.StmtExecContext = (*Stmt)(nil)
var _ driver.StmtQueryContext = (*Stmt)(nil)

type Stmt struct {
	conn    *Conn
	queries []string
}

// Close closes the statement.
func (s *Stmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *Stmt) NumInput() int {
	return -1
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	values := convertOrdinal(args)
	return s.ExecContext(context.Background(), values)
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	output := make([]*rdsdata.ExecuteStatementOutput, 0, len(s.queries))
	for _, query := range s.queries {
		out, err := s.executeStatement(ctx, query, args)
		if err != nil {
			return nil, err
		}
		output = append(output, out)
	}
	return newResult(output), nil
}

// Query executes a query that may return rows, such as a SELECT.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	values := convertOrdinal(args)
	return s.QueryContext(context.Background(), values)
}

// QueryContext executes a query that may return rows, such as a SELECT.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	output := make([]*rdsdata.ExecuteStatementOutput, 0, len(s.queries))
	for _, query := range s.queries {
		out, err := s.executeStatement(ctx, query, args)
		if err != nil {
			return nil, err
		}
		output = append(output, out)
	}
	return newRows(output), nil
}

// convertOrdinal converts the values to named values.
func convertOrdinal(values []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(values))
	for i, v := range values {
		namedValues[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return namedValues
}

func (s *Stmt) executeStatement(ctx context.Context, query string, _ []driver.NamedValue) (*rdsdata.ExecuteStatementOutput, error) {
	// TODO: Implement the conversion of named values to positional values.
	return s.conn.client.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{
		ResourceArn:           &s.conn.connector.resourceArn,
		SecretArn:             &s.conn.connector.secretArn,
		Database:              &s.conn.connector.database,
		Sql:                   &query,
		IncludeResultMetadata: true,
	})
}
