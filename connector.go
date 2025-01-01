package rdsdata

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
	"github.com/shogo82148/go-retry/v2"
)

var _ driver.Connector = (*Connector)(nil)

type Connector struct {
	driver *Driver
	cfg    *Config
	policy *retry.Policy
}

func NewConnector(cfg *Config) *Connector {
	return newConnector(NewDriver(), cfg)
}

func newConnector(driver *Driver, cfg *Config) *Connector {
	return &Connector{
		driver: driver,
		cfg:    cfg.Clone(),
		policy: &retry.Policy{
			MinDelay: time.Second,
			MaxDelay: 30 * time.Second,
			MaxCount: 5,
			Jitter:   time.Second,
		},
	}
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(c.cfg.AWSRegion))
	if err != nil {
		return nil, err
	}
	client := rdsdata.NewFromConfig(awsConfig)
	dialect, err := c.detectDatabaseEngine(ctx, client)
	if err != nil {
		return nil, err
	}
	return &Conn{
		client:    client,
		connector: c,
		dialect:   dialect,
	}, nil
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
}

func (c *Connector) detectDatabaseEngine(ctx context.Context, client awsClientInterface) (Dialect, error) {
	in := &rdsdata.ExecuteStatementInput{
		ResourceArn: &c.cfg.ResourceArn,
		SecretArn:   &c.cfg.SecretArn,
		Database:    &c.cfg.Database,
		Sql:         aws.String("SELECT VERSION()"),
	}

	return retry.DoValue(ctx, c.policy, func() (Dialect, error) {
		out, err := client.ExecuteStatement(ctx, in)
		if err != nil {
			return nil, err
		}
		if len(out.Records) == 0 {
			return nil, errors.New("rdsdata: invalid response to version request")
		}

		row := out.Records[0]
		if len(row) == 0 {
			return nil, errors.New("rdsdata: invalid response to version request")
		}

		field := row[0]
		version, ok := field.(*types.FieldMemberStringValue)
		if !ok {
			return nil, errors.New("rdsdata: invalid response to version request")
		}

		if strings.Contains(strings.ToLower(version.Value), "postgresql") {
			return c.newDialectPostgres(), nil
		}

		return c.newDialectMySQL(), nil
	})
}

func (c *Connector) newDialectMySQL() *DialectMySQL {
	loc := time.UTC
	if c.cfg.Location != nil {
		loc = c.cfg.Location
	}
	return &DialectMySQL{
		location:     loc,
		parseTime:    c.cfg.ParseTime,
		timeTruncate: c.cfg.TimeTruncate,
	}
}

func (c *Connector) newDialectPostgres() *DialectPostgres {
	return &DialectPostgres{}
}
