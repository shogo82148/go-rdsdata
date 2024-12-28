package rdsdata

import "testing"

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
	cfg := &Config{
		ResourceArn: "resourceARN",
		SecretArn:   "SecretARN",
		Database:    "database",
		AWSRegion:   "region",
	}
	dsn := cfg.FormatDSN()
	if dsn != "rdsdata://?aws_region=region&database=database&resource_arn=resourceARN&secret_arn=SecretARN" {
		t.Errorf("unexpected DSN: %s", dsn)
	}
}
