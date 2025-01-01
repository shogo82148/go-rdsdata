package rdsdata

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

const (
	keyResourceARN  = "resource_arn"
	keySecretARN    = "secret_arn"
	keyDatabase     = "database"
	keyAWSRegion    = "aws_region"
	keyLocation     = "location"
	keyParseTime    = "parse_time"
	keyTimeTruncate = "time_truncate"
)

// ErrInvalidDSNScheme is returned when the DSN scheme is not valid.
var ErrInvalidDSNScheme = errors.New("rdsdata: invalid DSN scheme")

// Config is the configuration for the RDS Data API driver.
type Config struct {
	// ResourceArn is the Amazon Resource Name (ARN) of
	// the Aurora Serverless DB cluster or RDS DB instance.
	ResourceArn string

	// SecretArn is the Amazon Resource Name (ARN) of the secret
	// managed by Secrets Manager.
	SecretArn string

	// Database is the name of the database.
	Database string

	// AWSRegion is the AWS region.
	AWSRegion string

	// Location specifies the location for time.Time values.
	// The default is UTC.
	Location *time.Location

	// ParseTime parses time.Time values from the database.
	ParseTime bool

	// TimeTruncate truncates time.Time values to the nearest.
	TimeTruncate time.Duration
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
		case keyLocation:
			loc, err := time.LoadLocation(v)
			if err != nil {
				return nil, err
			}
			cfg.Location = loc
		case keyParseTime:
			parseTime, err := strconv.ParseBool(v)
			if err != nil {
				return nil, err
			}
			cfg.ParseTime = parseTime
		case keyTimeTruncate:
			timeTruncate, err := time.ParseDuration(v)
			if err != nil {
				return nil, err
			}
			cfg.TimeTruncate = timeTruncate
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
	if cfg.Location != nil {
		v.Add(keyLocation, cfg.Location.String())
	}
	if cfg.ParseTime {
		v.Add(keyParseTime, strconv.FormatBool(cfg.ParseTime))
	}
	if cfg.TimeTruncate != 0 {
		v.Add(keyTimeTruncate, cfg.TimeTruncate.String())
	}
	return "rdsdata://?" + v.Encode()
}

func (cfg *Config) Clone() *Config {
	return &Config{
		ResourceArn:  cfg.ResourceArn,
		SecretArn:    cfg.SecretArn,
		Database:     cfg.Database,
		AWSRegion:    cfg.AWSRegion,
		Location:     cfg.Location,
		ParseTime:    cfg.ParseTime,
		TimeTruncate: cfg.TimeTruncate,
	}
}
