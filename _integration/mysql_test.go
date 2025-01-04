package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/shogo82148/go-rdsdata"
)

// jst is the timezone for Japan Standard Time.
var jst = time.FixedZone("Asia/Tokyo", 9*60*60)

func runMySQLTest(t *testing.T, f func(ctx context.Context, t *testing.T, db *sql.DB)) {
	t.Parallel()
	dbname := fmt.Sprintf("test_%x", rand.Uint64())

	t.Run("rdsdata", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		// setup a connection to RDS Data API
		config0 := &rdsdata.Config{
			ResourceArn: os.Getenv("RDSDATA_MYSQL_RESOURCE_ARN"),
			SecretArn:   os.Getenv("RDSDATA_MYSQL_SECRET_ARN"),
			AWSRegion:   os.Getenv("AWS_REGION"),
		}

		connector0 := rdsdata.NewConnector(config0)
		db0 := sql.OpenDB(connector0)

		if _, err := db0.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbname); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := db0.ExecContext(ctx, "DROP DATABASE IF EXISTS "+dbname); err != nil {
				t.Fatal(err)
			}
			if err := db0.Close(); err != nil {
				t.Fatal(err)
			}
		})

		// setup a connection to the MySQL server via RDS Data API
		config := &rdsdata.Config{
			ResourceArn: os.Getenv("RDSDATA_MYSQL_RESOURCE_ARN"),
			SecretArn:   os.Getenv("RDSDATA_MYSQL_SECRET_ARN"),
			Database:    dbname,
			AWSRegion:   os.Getenv("AWS_REGION"),
			Location:    jst,
			ParseTime:   true,
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
		config0 := mysql.NewConfig()
		config0.Addr = "127.0.0.1:3306"
		config0.Passwd = "supersecret"
		config0.Net = "tcp"
		config0.User = "root"

		connector0, err := mysql.NewConnector(config0)
		if err != nil {
			t.Fatal(err)
		}
		db0 := sql.OpenDB(connector0)

		if _, err := db0.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbname); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := db0.ExecContext(ctx, "DROP DATABASE IF EXISTS "+dbname); err != nil {
				t.Fatal(err)
			}
			if err := db0.Close(); err != nil {
				t.Fatal(err)
			}
		})

		// setup a connection to the local MySQL server
		config := config0.Clone()
		config.DBName = dbname
		config.Loc = jst
		config.ParseTime = true
		connector, err := mysql.NewConnector(config)
		if err != nil {
			t.Fatal(err)
		}

		db := sql.OpenDB(connector)
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})

		// run tests
		f(t.Context(), t, db)
	})
}

func TestMySQL_Ping(t *testing.T) {
	runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
		if err := db.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMySQL_Select(t *testing.T) {
	runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
		rows, err := db.QueryContext(ctx, "SELECT 1 AS one")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		// check columns
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		if len(columns) != 1 {
			t.Errorf("unexpected columns: %v", columns)
		}
		if columns[0] != "one" {
			t.Errorf("unexpected columns: %v", columns)
		}

		// check rows
		if !rows.Next() {
			t.Fatal("no rows")
		}
		var one any
		if err := rows.Scan(&one); err != nil {
			t.Fatal(err)
		}
		if one != int64(1) {
			t.Errorf("unexpected value: %v", one)
		}

		// check no more rows
		if rows.Next() {
			t.Fatal("more rows")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMySQL_ConvertParameters(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			row := db.QueryRowContext(ctx, "SELECT ?", 42)

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != int64(42) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("float64", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			row := db.QueryRowContext(ctx, "SELECT ?", 42.0)

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != 42.0 {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("bool", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			row := db.QueryRowContext(ctx, "SELECT ?", true)

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != int64(1) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("bytes", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			row := db.QueryRowContext(ctx, "SELECT ?", []byte{0x01, 0x02, 0x03})

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte{0x01, 0x02, 0x03}) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("string", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			row := db.QueryRowContext(ctx, "SELECT ?", "hello")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("hello")) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("time.Time", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			row := db.QueryRowContext(ctx, "SELECT ?", time.Date(2021, 1, 2, 3, 4, 5, 999_999_999, time.UTC))

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("2021-01-02 12:04:05.999999999")) {
				t.Errorf("unexpected value: %s, %T", value, value)
			}
		})
	})

	t.Run("nil", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			row := db.QueryRowContext(ctx, "SELECT ?", nil)

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != nil {
				t.Errorf("unexpected value: %s, %T", value, value)
			}
		})
	})

	t.Run("json.RawMessage", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			row := db.QueryRowContext(ctx, "SELECT ?", json.RawMessage(`{"hello": "world"}`))

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte(`{"hello": "world"}`)) {
				t.Errorf("unexpected value: %s, %T", value, value)
			}
		})
	})
}

func TestMySQL_ConvertResult(t *testing.T) {
	t.Run("BIT", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value BIT(6))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (b'101')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			// go-sql-driver/mysql converts BIT to []byte
			// however, RDS Data API converts BIT to bool
			if data, ok := value.([]byte); (!ok || !bytes.Equal(data, []byte{0x05})) && value != int64(1) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("TINYINT", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value TINYINT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (42)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != int64(42) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("TINYINT UNSIGNED", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value TINYINT UNSIGNED)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (42)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != int64(42) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("BOOLEAN", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value BOOLEAN)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (TRUE)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}

			if value != int64(1) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("SMALLINT", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value SMALLINT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (42)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != int64(42) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("SMALLINT UNSIGNED", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value SMALLINT UNSIGNED)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (42)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != int64(42) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("MEDIUMINT", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value MEDIUMINT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (42)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != int64(42) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("MEDIUMINT UNSIGNED", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value MEDIUMINT UNSIGNED)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (42)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != int64(42) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("INT", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value INT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (42)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != int64(42) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("INT UNSIGNED", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value INT UNSIGNED)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (42)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != int64(42) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("BIGINT", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value BIGINT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (42)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != int64(42) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("BIGINT UNSIGNED", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value BIGINT UNSIGNED)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES ('9223372036854775807')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != uint64(9223372036854775807) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("BIGINT UNSIGNED NULL", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value BIGINT UNSIGNED)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (NULL)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != nil {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("DECIMAL", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value DECIMAL(5,2))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (0.1)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("0.10")) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("FLOAT", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value FLOAT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (42)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != float32(42.0) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("FLOAT NULL", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value FLOAT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (NULL)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != nil {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("DOUBLE", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value DOUBLE)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (42)"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != 42.0 {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("CHAR", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value CHAR(20))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES ('hello')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("hello")) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("VARCHAR", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value VARCHAR(20))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES ('hello')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("hello")) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("BINARY", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value BINARY(10))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES ('hello')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("hello\x00\x00\x00\x00\x00")) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("VARBINARY", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value VARBINARY(10))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES ('hello')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("hello")) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("TINYTEXT", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value TINYTEXT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES ('hello')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("hello")) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("TEXT", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value TEXT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES ('hello')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("hello")) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("MEDIUMTEXT", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value MEDIUMTEXT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES ('hello')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("hello")) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("LONGTEXT", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value LONGTEXT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES ('hello')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("hello")) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("TINYBLOB", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value TINYBLOB)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (X'010203')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte{0x01, 0x02, 0x03}) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("BLOB", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value BLOB)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (X'010203')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte{0x01, 0x02, 0x03}) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("MEDIUMBLOB", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value MEDIUMBLOB)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (X'010203')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte{0x01, 0x02, 0x03}) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("LONGBLOB", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value LONGBLOB)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (value) VALUES (X'010203')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte{0x01, 0x02, 0x03}) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("ENUM", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (color ENUM('red', 'green', 'blue'))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (color) VALUES ('red')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT color FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("red")) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("SET", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (color SET('red', 'green', 'blue'))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO test (color) VALUES ('red,green')"); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT color FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte("red,green")) {
				t.Errorf("unexpected value: %q", value)
			}
		})
	})

	t.Run("JSON", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (json JSON)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO test (json) VALUES ('{}')`); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT json FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value.([]byte), []byte(`{}`)) {
				t.Errorf("unexpected value: %q", value)
			}
		})
	})

	t.Run("DATE", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value DATE)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO test (value) VALUES ('2025-01-04')`); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}

			// go-sql-driver/mysql converts DATETIME to time.Time if ParseTime=true.
			tv, ok := value.(time.Time)
			if !ok {
				t.Errorf("unexpected value: %T", value)
			}
			if !tv.Equal(time.Date(2025, 01, 04, 0, 0, 0, 0, jst)) {
				t.Errorf("unexpected value: %q", value)
			}
		})
	})

	t.Run("TIME", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value TIME(6))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO test (value) VALUES ('12:34:56.789012')`); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}

			tv, ok := value.([]byte)
			if !ok {
				t.Errorf("unexpected value: %T", value)
			}
			if !bytes.Equal(tv, []byte("12:34:56.789012")) {
				t.Errorf("unexpected value: %q", value)
			}
		})
	})

	t.Run("DATETIME", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value DATETIME(6))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO test (value) VALUES ('9999-12-31 23:59:59.999999')`); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}

			// go-sql-driver/mysql converts DATETIME to time.Time if ParseTime=true.
			tv, ok := value.(time.Time)
			if !ok {
				t.Errorf("unexpected value: %T", value)
			}
			if !tv.Equal(time.Date(9999, 12, 31, 23, 59, 59, 999_999_000, jst)) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})

	t.Run("TIMESTAMP", func(t *testing.T) {
		runMySQLTest(t, func(ctx context.Context, t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, "CREATE TABLE test (value TIMESTAMP(6))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO test (value) VALUES ('2038-01-19 03:14:07.999999')`); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRowContext(ctx, "SELECT value FROM test")

			var value any
			if err := row.Scan(&value); err != nil {
				t.Fatal(err)
			}

			// go-sql-driver/mysql converts TIMESTAMP to time.Time if ParseTime=true.
			tv, ok := value.(time.Time)
			if !ok {
				t.Errorf("unexpected value: %T", value)
			}
			if !tv.Equal(time.Date(2038, 1, 19, 3, 14, 7, 999_999_000, jst)) {
				t.Errorf("unexpected value: %v", value)
			}
		})
	})
}
