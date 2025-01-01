package rdsdata

import (
	"testing"
	"time"
)

func TestParseDSN(t *testing.T) {
	t.Run("can parse DSN", func(t *testing.T) {
		dsn := "rdsdata://?resource_arn=resourceARN&secret_arn=secretARN&database=database&aws_region=region"
		cfg, err := ParseDSN(dsn)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.ResourceArn != "resourceARN" {
			t.Errorf("unexpected ResourceArn: %s", cfg.ResourceArn)
		}
		if cfg.SecretArn != "secretARN" {
			t.Errorf("unexpected SecretArn: %s", cfg.SecretArn)
		}
		if cfg.Database != "database" {
			t.Errorf("unexpected Database: %s", cfg.Database)
		}
		if cfg.AWSRegion != "region" {
			t.Errorf("unexpected AWSRegion: %s", cfg.AWSRegion)
		}
	})

	t.Run("location", func(t *testing.T) {
		dns := "rdsdata://?location=Asia%2FTokyo"
		cfg, err := ParseDSN(dns)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Location.String() != "Asia/Tokyo" {
			t.Errorf("unexpected Location: %v", cfg.Location)
		}
	})

	t.Run("parseTime", func(t *testing.T) {
		dns := "rdsdata://?parse_time=true"
		cfg, err := ParseDSN(dns)
		if err != nil {
			t.Fatal(err)
		}
		if !cfg.ParseTime {
			t.Errorf("unexpected ParseTime: %v", cfg.ParseTime)
		}
	})

	t.Run("timeTruncate", func(t *testing.T) {
		dns := "rdsdata://?time_truncate=1s"
		cfg, err := ParseDSN(dns)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.TimeTruncate != time.Second {
			t.Errorf("unexpected TimeTruncate: %v", cfg.TimeTruncate)
		}
	})

	t.Run("returns error when the DSN scheme is invalid", func(t *testing.T) {
		dsn := "invalid://?resource_arn=resourceARN&secret_arn=secretARN&database=database&aws_region=region"
		_, err := ParseDSN(dsn)
		if err != ErrInvalidDSNScheme {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("returns error when unknown parameter is passed", func(t *testing.T) {
		dsn := "rdsdata://?unknown=unknown"
		_, err := ParseDSN(dsn)
		if err == nil {
			t.Fatal("expected error, but got nil")
		}
	})
}

func TestConfig_FormatDSN(t *testing.T) {
	testCases := []struct {
		name string
		cfg  *Config
		want string
	}{
		{
			name: "basic fields",
			cfg: &Config{
				ResourceArn: "resourceARN",
				SecretArn:   "SecretARN",
				Database:    "database",
				AWSRegion:   "region",
			},
			want: "rdsdata://?aws_region=region&database=database&resource_arn=resourceARN&secret_arn=SecretARN",
		},
		{
			name: "location",
			cfg: &Config{
				ResourceArn: "resourceARN",
				SecretArn:   "SecretARN",
				AWSRegion:   "region",
				Location:    time.UTC,
			},
			want: "rdsdata://?aws_region=region&location=UTC&resource_arn=resourceARN&secret_arn=SecretARN",
		},
		{
			name: "parseTime",
			cfg: &Config{
				ResourceArn: "resourceARN",
				SecretArn:   "SecretARN",
				AWSRegion:   "region",
				ParseTime:   true,
			},
			want: "rdsdata://?aws_region=region&parse_time=true&resource_arn=resourceARN&secret_arn=SecretARN",
		},
		{
			name: "timeTruncate",
			cfg: &Config{
				ResourceArn:  "resourceARN",
				SecretArn:    "SecretARN",
				AWSRegion:    "region",
				TimeTruncate: time.Second,
			},
			want: "rdsdata://?aws_region=region&resource_arn=resourceARN&secret_arn=SecretARN&time_truncate=1s",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.cfg.FormatDSN()
			if got != tc.want {
				t.Errorf("unexpected DSN: %s, want %s", got, tc.want)
			}
		})
	}
}
