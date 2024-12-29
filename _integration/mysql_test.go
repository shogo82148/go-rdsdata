package integration

import (
	"context"
	"database/sql"
	"os"
	"testing"

	rdsdata "github.com/shogo82148/go-rdsdata"
)

func TestDriver_Open(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("rdsdata", newConfig().FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConn_OpenDB(t *testing.T) {
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

func TestQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connector := rdsdata.NewConnector(newConfig())
	db := sql.OpenDB(connector)
	defer db.Close()

	rows, err := db.QueryContext(ctx, "SELECT 1 AS one, 2 as two")
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		var database string
		if err := rows.Scan(&database); err != nil {
			t.Fatal(err)
		}
		t.Log(database)
	}
}
