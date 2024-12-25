package rdsdata

import (
	"context"
	"database/sql/driver"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
)

var _ driver.Connector = (*Connector)(nil)

type Connector struct {
	driver      *Driver
	resourceArn string
	secretArn   string
	database    string
	awsRegion   string
}

func NewConnector(cfg *Config) *Connector {
	return &Connector{
		driver:      NewDriver(),
		resourceArn: cfg.ResourceArn,
		secretArn:   cfg.SecretArn,
		database:    cfg.Database,
		awsRegion:   cfg.AWSRegion,
	}
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(c.awsRegion))
	if err != nil {
		return nil, err
	}
	client := rdsdata.NewFromConfig(awsConfig)
	return &Conn{
		client:    client,
		connector: c,
	}, nil
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
}
