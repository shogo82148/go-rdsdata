package rdsdata

import (
	"database/sql"
	"database/sql/driver"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
)

var postgresRegex = regexp.MustCompile(`\$([0-9]+)`)

// compile time type check
var _ Dialect = (*DialectPostgres)(nil)

// DialectPostgres is the PostgreSQL dialect.
type DialectPostgres struct{}

// MigrateQuery converts a PostgreSQL query into an RDS statement.
func (d *DialectPostgres) MigrateQuery(query string, args []driver.NamedValue) (*rdsdata.ExecuteStatementInput, error) {
	ordinal, err := isOrdinal(args)
	if err != nil {
		return nil, err
	}

	if !ordinal {
		params, err := convertNamedValues(args)
		if err != nil {
			return nil, err
		}
		return &rdsdata.ExecuteStatementInput{
			Parameters: params,
			Sql:        aws.String(query),
		}, nil
	}

	// PostgreSQL uses $1, $2, etc. for placeholders, so we need to convert the ordinal placeholders to named placeholders.
	namedArgs := convertOrdinalToNamed(args)
	query = postgresRegex.ReplaceAllStringFunc(query, func(s string) string {
		return ":" + s[1:]
	})

	params, err := convertNamedValues(namedArgs)
	if err != nil {
		return nil, err
	}
	return &rdsdata.ExecuteStatementInput{
		Parameters: params,
		Sql:        aws.String(query),
	}, nil
}

func (d *DialectPostgres) IsIsolationLevelSupported(level sql.IsolationLevel) bool {
	switch level {
	case sql.LevelDefault:
		return true
	case sql.LevelRepeatableRead:
		return true
	case sql.LevelReadCommitted:
		return true
	case sql.LevelReadUncommitted:
		return true
	case sql.LevelSerializable:
		return true
	default:
		return false
	}
}

func (d *DialectPostgres) GetFieldConverter(columnType string) FieldConverter {
	return convertDefault
}
