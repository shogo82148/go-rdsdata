package rdsdata

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
)

var _ awsClientInterface = (*awsClientMock)(nil)

type awsClientMock struct {
	ExecuteStatementFunc    func(ctx context.Context, e *rdsdata.ExecuteStatementInput, optFns ...func(*rdsdata.Options)) (*rdsdata.ExecuteStatementOutput, error)
	BeginTransactionFunc    func(ctx context.Context, b *rdsdata.BeginTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.BeginTransactionOutput, error)
	CommitTransactionFunc   func(ctx context.Context, c *rdsdata.CommitTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.CommitTransactionOutput, error)
	RollbackTransactionFunc func(ctx context.Context, r *rdsdata.RollbackTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.RollbackTransactionOutput, error)
}

func (mock *awsClientMock) ExecuteStatement(ctx context.Context, e *rdsdata.ExecuteStatementInput, optFns ...func(*rdsdata.Options)) (*rdsdata.ExecuteStatementOutput, error) {
	return mock.ExecuteStatementFunc(ctx, e, optFns...)
}

func (mock *awsClientMock) BeginTransaction(ctx context.Context, b *rdsdata.BeginTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.BeginTransactionOutput, error) {
	return mock.BeginTransactionFunc(ctx, b, optFns...)
}

func (mock *awsClientMock) CommitTransaction(ctx context.Context, c *rdsdata.CommitTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.CommitTransactionOutput, error) {
	return mock.CommitTransactionFunc(ctx, c, optFns...)
}

func (mock *awsClientMock) RollbackTransaction(ctx context.Context, r *rdsdata.RollbackTransactionInput, optFns ...func(*rdsdata.Options)) (*rdsdata.RollbackTransactionOutput, error) {
	return mock.RollbackTransactionFunc(ctx, r, optFns...)
}

func TestConn_Ping(t *testing.T) {
	client := &awsClientMock{
		ExecuteStatementFunc: func(ctx context.Context, input *rdsdata.ExecuteStatementInput, optFns ...func(*rdsdata.Options)) (*rdsdata.ExecuteStatementOutput, error) {
			if aws.ToString(input.ResourceArn) != "resourceArn" {
				t.Errorf("unexpected ResourceArn: %s", aws.ToString(input.ResourceArn))
			}
			if aws.ToString(input.SecretArn) != "secretArn" {
				t.Errorf("unexpected SecretArn: %s", aws.ToString(input.SecretArn))
			}
			if aws.ToString(input.Database) != "database" {
				t.Errorf("unexpected Database: %s", aws.ToString(input.Database))
			}
			if aws.ToString(input.Sql) != "/* ping */ SELECT 1" {
				t.Errorf("unexpected SQL: %s", aws.ToString(input.Sql))
			}
			return nil, nil
		},
	}
	conn := &Conn{
		client: client,
		connector: &Connector{
			resourceArn: "resourceArn",
			secretArn:   "secretArn",
			database:    "database",
		},
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}
