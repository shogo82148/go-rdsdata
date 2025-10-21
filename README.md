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

## License

MIT

## Prior Works

- <https://github.com/krotscheck/go-rds-driver>
