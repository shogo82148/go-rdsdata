package integration

import (
	"context"
	"database/sql"
	"os"
	"testing"

	rdsdata "github.com/shogo82148/go-rdsdata"
)

func TestConn_Ping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connector := rdsdata.NewConnector(newConfig())
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
}

func newConfig() *rdsdata.Config {
	return &rdsdata.Config{
		ResourceArn: os.Getenv("RDSDATA_RESOURCE_ARN"),
		SecretArn:   os.Getenv("RDSDATA_SECRET_ARN"),
		AWSRegion:   os.Getenv("AWS_REGION"),
	}
}
