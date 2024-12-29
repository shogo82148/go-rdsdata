package rdsdata

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
)

func TestTx_Commit(t *testing.T) {
	client := &awsClientMock{
		BeginTransactionFunc: func(ctx context.Context, input *rdsdata.BeginTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.BeginTransactionOutput, error) {
			if aws.ToString(input.ResourceArn) != "resourceArn" {
				t.Errorf("unexpected ResourceArn: %s", aws.ToString(input.ResourceArn))
			}
			if aws.ToString(input.SecretArn) != "secretArn" {
				t.Errorf("unexpected SecretArn: %s", aws.ToString(input.SecretArn))
			}
			if aws.ToString(input.Database) != "database" {
				t.Errorf("unexpected Database: %s", aws.ToString(input.Database))
			}
			return &rdsdata.BeginTransactionOutput{
				TransactionId: aws.String("transactionId"),
			}, nil
		},
		CommitTransactionFunc: func(ctx context.Context, input *rdsdata.CommitTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.CommitTransactionOutput, error) {
			if aws.ToString(input.ResourceArn) != "resourceArn" {
				t.Errorf("unexpected ResourceArn: %s", aws.ToString(input.ResourceArn))
			}
			if aws.ToString(input.SecretArn) != "secretArn" {
				t.Errorf("unexpected SecretArn: %s", aws.ToString(input.SecretArn))
			}
			if aws.ToString(input.TransactionId) != "transactionId" {
				t.Errorf("unexpected TransactionId: %s", aws.ToString(input.TransactionId))
			}
			return &rdsdata.CommitTransactionOutput{}, nil
		},
	}
	conn := &Conn{
		client: client,
		connector: &Connector{
			resourceArn: "resourceArn",
			secretArn:   "secretArn",
			database:    "database",
		},
		dialect: &DialectMySQL{},
	}
	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestTx_Rollback(t *testing.T) {
	client := &awsClientMock{
		BeginTransactionFunc: func(ctx context.Context, input *rdsdata.BeginTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.BeginTransactionOutput, error) {
			if aws.ToString(input.ResourceArn) != "resourceArn" {
				t.Errorf("unexpected ResourceArn: %s", aws.ToString(input.ResourceArn))
			}
			if aws.ToString(input.SecretArn) != "secretArn" {
				t.Errorf("unexpected SecretArn: %s", aws.ToString(input.SecretArn))
			}
			if aws.ToString(input.Database) != "database" {
				t.Errorf("unexpected Database: %s", aws.ToString(input.Database))
			}
			return &rdsdata.BeginTransactionOutput{
				TransactionId: aws.String("transactionId"),
			}, nil
		},
		RollbackTransactionFunc: func(ctx context.Context, input *rdsdata.RollbackTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.RollbackTransactionOutput, error) {
			if aws.ToString(input.ResourceArn) != "resourceArn" {
				t.Errorf("unexpected ResourceArn: %s", aws.ToString(input.ResourceArn))
			}
			if aws.ToString(input.SecretArn) != "secretArn" {
				t.Errorf("unexpected SecretArn: %s", aws.ToString(input.SecretArn))
			}
			if aws.ToString(input.TransactionId) != "transactionId" {
				t.Errorf("unexpected TransactionId: %s", aws.ToString(input.TransactionId))
			}
			return &rdsdata.RollbackTransactionOutput{}, nil
		},
	}
	conn := &Conn{
		client: client,
		connector: &Connector{
			resourceArn: "resourceArn",
			secretArn:   "secretArn",
			database:    "database",
		},
		dialect: &DialectMySQL{},
	}
	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
