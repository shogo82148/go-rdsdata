package rdsdata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
)

// compile time type check
var _ driver.Conn = (*Conn)(nil)
var _ driver.ConnPrepareContext = (*Conn)(nil)
var _ driver.ConnBeginTx = (*Conn)(nil)
var _ driver.Pinger = (*Conn)(nil)
var _ driver.ExecerContext = (*Conn)(nil)
var _ driver.QueryerContext = (*Conn)(nil)

// awsClientInterface interface that captures methods required by the driver. In this case, replicating the RDS API
type awsClientInterface interface {
	ExecuteStatement(ctx context.Context, e *rdsdata.ExecuteStatementInput, optFns ...func(*rdsdata.Options)) (*rdsdata.ExecuteStatementOutput, error)
	BeginTransaction(ctx context.Context, b *rdsdata.BeginTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.BeginTransactionOutput, error)
	CommitTransaction(ctx context.Context, c *rdsdata.CommitTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.CommitTransactionOutput, error)
	RollbackTransaction(ctx context.Context, r *rdsdata.RollbackTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.RollbackTransactionOutput, error)
}

type Conn struct {
	client    awsClientInterface
	connector *Connector
	dialect   Dialect

	// Tx is the current transaction.
	tx *Tx
}

// Prepare prepares a query.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.prepareContext(query)
}

// PrepareContext prepares a query.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.prepareContext(query)
}

func (c *Conn) prepareContext(query string) (*Stmt, error) {
	stmt := &Stmt{
		conn:    c,
		queries: []string{query},
	}
	return stmt, nil
}

// Close closes the connection.
func (c *Conn) Close() error {
	return nil
}

// Begin begins a transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{
		Isolation: driver.IsolationLevel(sql.LevelDefault),
		ReadOnly:  false,
	})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	level := sql.IsolationLevel(opts.Isolation)
	if !c.dialect.IsIsolationLevelSupported(level) {
		return nil, fmt.Errorf("rdsdata: unsupported isolation level: %s", level.String())
	}

	out, err := c.client.BeginTransaction(ctx, &rdsdata.BeginTransactionInput{
		ResourceArn: &c.connector.resourceArn,
		SecretArn:   &c.connector.secretArn,
		Database:    &c.connector.database,
	})
	if err != nil {
		return nil, err
	}

	tx := &Tx{
		ctx:  ctx,
		id:   out.TransactionId,
		conn: c,
	}

	var clause []string
	if level != sql.LevelDefault {
		clause = append(clause, "ISOLATION LEVEL "+level.String())
	}
	if opts.ReadOnly {
		clause = append(clause, "READ ONLY")
	}
	if len(clause) > 0 {
		if _, err := c.client.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{
			ResourceArn: &c.connector.resourceArn,
			SecretArn:   &c.connector.secretArn,
			Database:    &c.connector.database,
			Sql:         aws.String("SET TRANSACTION " + strings.Join(clause, ", ")),
		}); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}

	c.tx = tx
	return tx, nil
}

// ExecContext executes a query.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := c.prepareContext(query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args)
}

// QueryContext executes a query.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := c.prepareContext(query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args)
}

// Ping ping the database to check if the connection is still alive.
func (c *Conn) Ping(ctx context.Context) error {
	_, err := c.client.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{
		ResourceArn: &c.connector.resourceArn,
		SecretArn:   &c.connector.secretArn,
		Database:    &c.connector.database,
		Sql:         aws.String("/* ping */ SELECT 1"),
	})
	return err
}
