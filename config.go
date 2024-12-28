package rdsdata

import (
	"errors"
	"fmt"
	"net/url"
)

const (
	keyResourceARN = "resource_arn"
	keySecretARN   = "secret_arn"
	keyDatabase    = "database"
	keyAWSRegion   = "aws_region"
)

// ErrInvalidDSNScheme is returned when the DSN scheme is not valid.
var ErrInvalidDSNScheme = errors.New("rdsdata: invalid DSN scheme")

// Config is the configuration for the RDS Data API driver.
type Config struct {
	ResourceArn string
	SecretArn   string
	Database    string
	AWSRegion   string
}

// ParseDSN parses the DSN string to a Config.
func ParseDSN(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "rdsdata" {
		return nil, ErrInvalidDSNScheme
	}

	var cfg Config
	query := u.Query()
	for k := range query {
		v := query.Get(k)
		switch k {
		case keyResourceARN:
			cfg.ResourceArn = v
		case keySecretARN:
			cfg.SecretArn = v
		case keyDatabase:
			cfg.Database = v
		case keyAWSRegion:
			cfg.AWSRegion = v
		default:
			return nil, fmt.Errorf("rdsdata: unknown parameter %q", k)
		}
	}
	return &cfg, nil
}

// FormatDSN formats the Config to a DSN string.
func (cfg *Config) FormatDSN() string {
	v := url.Values{}
	v.Add(keyResourceARN, cfg.ResourceArn)
	v.Add(keySecretARN, cfg.SecretArn)
	v.Add(keyDatabase, cfg.Database)
	v.Add(keyAWSRegion, cfg.AWSRegion)
	return "rdsdata://?" + v.Encode()
}
