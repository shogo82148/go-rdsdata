package rdsdata

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
)

// compile time type check
var _ driver.Tx = (*Tx)(nil)

type Tx struct {
	ctx  context.Context
	id   *string
	conn *Conn
	done bool
}

func (tx *Tx) Commit() error {
	if tx.done {
		return sql.ErrTxDone
	}

	_, err := tx.conn.client.CommitTransaction(tx.ctx, &rdsdata.CommitTransactionInput{
		ResourceArn:   &tx.conn.connector.cfg.ResourceArn,
		SecretArn:     &tx.conn.connector.cfg.SecretArn,
		TransactionId: tx.id,
	})
	if err != nil {
		return err
	}

	tx.conn.tx = nil
	tx.done = true
	return nil
}

func (tx *Tx) Rollback() error {
	if tx.done {
		return sql.ErrTxDone
	}

	ctx := context.WithoutCancel(tx.ctx)
	_, err := tx.conn.client.RollbackTransaction(ctx, &rdsdata.RollbackTransactionInput{
		ResourceArn:   &tx.conn.connector.cfg.ResourceArn,
		SecretArn:     &tx.conn.connector.cfg.SecretArn,
		TransactionId: tx.id,
	})
	if err != nil {
		return err
	}

	tx.conn.tx = nil
	tx.done = true
	return nil
}
