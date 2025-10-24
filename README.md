[![test](https://github.com/shogo82148/go-rdsdata/actions/workflows/test.yaml/badge.svg)](https://github.com/shogo82148/go-rdsdata/actions/workflows/test.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/shogo82148/go-rdsdata.svg)](https://pkg.go.dev/github.com/shogo82148/go-rdsdata)

# go-rdsdata

A Go SQL Driver for the Amazon Aurora Serverless data api.

## Synopsis

```go
import (
  "context"
  "database/sql"
  "log"

  rdsdata "github.com/shogo82148/go-rdsdata"
)

func main() {
  // Connection settings
  config := &rdsdata.Config{
    // The ARN of Aurora Serverless DB cluster or RDS DB instance.
    ResourceArn: "arn:aws:rds:ap-northeast-1:123456789012:cluster:xxxxxx",

    // The ARN of the secret managed by Secrets Manager.
    SecretArn: "arn:aws:secretsmanager:ap-northeast-1:123456789012:secret:xxxxxx",

    // AWS Region
    AWSRegion: "ap-northeast-1",
  }

  // create a connection pool
  connector := rdsdata.NewConnector(config)
  db := sql.OpenDB(connector)

  // You can use it like a general driver for Go.
  if _, err := db.ExecContext(context.Background(), "CREATE DATABASE IF NOT EXISTS `test`"); err != nil {
    log.Fatal(err)
  }
}
```

## Data Mappings

### MySQL

The RDS MySQL version supported is 8.0. Driver parity is tested using `github.com/go-sql-driver/mysql`.

| Column Type  | RDS Data API Behavior                                                                                                                                                                |
| :----------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Unsigned Int | Not natively supported by the AWS SDK's Data API, and are all converted to the int64 type. As such large integer values may be lossy.                                                |
| `BIT(M)`     | The `BIT` column type is returned from RDS as a Boolean, preventing the full use of `BIT(M)`. Until (if ever) this is fixed, only `BIT(1)` column values are supported.              |
| `TINYINT(1)` | Declaring a `TINYINT(1)` in your table will cause the Data API to return a Boolean instead of an integer. Numeric values are only returned by `TINYINT(2)` or greater.               |
| `BOOLEAN`    | The `BOOLEAN` column type is converted into a `BIT` column by RDS.                                                                                                                   |
| Booleans     | Boolean marshalling and unmarshalling via `sql.*`, because of the above issues, only works reliably with the `TINYINT(2)` column type. Do not use `BOOLEAN`, `BIT`, or `TINYINT(1)`. |

### PostgreSQL

The RDS Postgres version supported is 16.6. Driver parity is tested using `github.com/jackc/pgx/v5`.

| Feature       | Limitation                                                                                                                                 |
| :------------ | :----------------------------------------------------------------------------------------------------------------------------------------- |
| Unsigned Int  | Not natively supported by the AWS SDK's Data API, and are all converted to the int64 type. As such large integer values may be lossy.      |
| Complex Types | Postgres complex types - in short anything in [section 8.8](https://www.postgresql.org/docs/10/datatype.html) and after, is not supported. |

## License

MIT

## Prior Works

- <https://github.com/krotscheck/go-rds-driver>
