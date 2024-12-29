package integration

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/shogo82148/go-rdsdata"
)

func runPostgresTest(t *testing.T, f func(ctx context.Context, t *testing.T, db *sql.DB)) {
	t.Parallel()
	dbname := fmt.Sprintf("test_%x", rand.Uint64())

	t.Run("rdsdata", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		// setup a connection to RDS Data API
		config0 := &rdsdata.Config{
			ResourceArn: os.Getenv("RDSDATA_POSTGRES_RESOURCE_ARN"),
			SecretArn:   os.Getenv("RDSDATA_POSTGRES_SECRET_ARN"),
			AWSRegion:   os.Getenv("AWS_REGION"),
		}

		connector0 := rdsdata.NewConnector(config0)
		db0 := sql.OpenDB(connector0)

		if _, err := db0.ExecContext(ctx, "CREATE DATABASE "+dbname); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := db0.ExecContext(ctx, "DROP DATABASE "+dbname+" WITH (FORCE)"); err != nil {
				t.Fatal(err)
			}
			if err := db0.Close(); err != nil {
				t.Fatal(err)
			}
		})

		// setup a connection to the MySQL server via RDS Data API
		config := &rdsdata.Config{
			ResourceArn: os.Getenv("RDSDATA_POSTGRES_RESOURCE_ARN"),
			SecretArn:   os.Getenv("RDSDATA_POSTGRES_SECRET_ARN"),
			Database:    dbname,
			AWSRegion:   os.Getenv("AWS_REGION"),
		}

		connector := rdsdata.NewConnector(config)
		db := sql.OpenDB(connector)
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})

		// run tests
		f(t.Context(), t, db)
	})

	t.Run("local", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		// setup a local MySQL server
		db0, err := sql.Open("pgx", "postgres://root:supersecret@localhost:5432")
		if err != nil {
			t.Fatal(err)
		}

		if _, err := db0.ExecContext(ctx, "CREATE DATABASE "+dbname); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			time.Sleep(1 * time.Second)
			if _, err := db0.ExecContext(ctx, "DROP DATABASE "+dbname+" WITH (FORCE)"); err != nil {
				t.Fatal(err)
			}
			if err := db0.Close(); err != nil {
				t.Fatal(err)
			}
		})

		// setup a connection to the local MySQL server
		db, err := sql.Open("pgx", "postgres://root:supersecret@localhost:5432/"+dbname)
		if err != nil {
			t.Fatal(err)
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})

		// run tests
		f(t.Context(), t, db)
	})
}

func TestPostgres_Ping(t *testing.T) {
	runPostgresTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
		if err := db.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
	})
}
