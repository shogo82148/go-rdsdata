package rdsdata

// Config is the configuration for the RDS Data API driver.
type Config struct {
	ResourceArn string
	SecretArn   string
	Database    string
	AWSRegion   string
}
