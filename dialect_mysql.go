package rdsdata

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

var ordinalRegex = regexp.MustCompile(`\?`)

// compile time type check
var _ Dialect = (*DialectMySQL)(nil)

// DialectMySQL is the MySQL dialect.
type DialectMySQL struct{}

// MigrateQuery converts a MySQL query into an RDS statement.
func (d *DialectMySQL) MigrateQuery(query string, args []driver.NamedValue) (*rdsdata.ExecuteStatementInput, error) {
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

	// MySQL uses ? for placeholders, so we need to convert the ordinal placeholders to named placeholders.
	namedArgs := convertOrdinalToNamed(args)
	idx := 0
	query = ordinalRegex.ReplaceAllStringFunc(query, func(string) string {
		idx++
		return ":" + strconv.Itoa(idx)
	})

	params, err := d.convertNamedValues(namedArgs)
	if err != nil {
		return nil, err
	}
	return &rdsdata.ExecuteStatementInput{
		Parameters: params,
		Sql:        aws.String(query),
	}, nil
}

// convertNamedValues converts named arguments to RDS parameters.
func (d *DialectMySQL) convertNamedValues(args []driver.NamedValue) ([]types.SqlParameter, error) {
	params := make([]types.SqlParameter, len(args))
	for i, arg := range args {
		sqlParam, err := d.convertNamedValue(arg)
		if err != nil {
			return nil, err
		}
		params[i] = sqlParam
	}
	return params, nil
}

// convertNamedValue converts a named argument to an RDS parameter.
func (d *DialectMySQL) convertNamedValue(arg driver.NamedValue) (types.SqlParameter, error) {
	name := arg.Name

	switch v := arg.Value.(type) {
	case int64:
		return types.SqlParameter{
			Name:  &name,
			Value: &types.FieldMemberLongValue{Value: v},
		}, nil
	case float64:
		return types.SqlParameter{
			Name:  &name,
			Value: &types.FieldMemberDoubleValue{Value: v},
		}, nil
	case bool:
		// In MySQL, TRUE is 1 and FALSE is 0.
		var w int64
		if v {
			w = 1
		}
		return types.SqlParameter{
			Name:  &name,
			Value: &types.FieldMemberLongValue{Value: w},
		}, nil
	case []byte:
		return types.SqlParameter{
			Name:  &name,
			Value: &types.FieldMemberBlobValue{Value: v},
		}, nil
	case string:
		return types.SqlParameter{
			Name:  &name,
			Value: &types.FieldMemberStringValue{Value: v},
		}, nil
	case time.Time:
		return types.SqlParameter{
			Name:     &name,
			TypeHint: types.TypeHintTimestamp,
			Value:    &types.FieldMemberStringValue{Value: v.Format(time.RFC3339Nano)},
		}, nil
	case nil:
		return types.SqlParameter{
			Name:  &name,
			Value: &types.FieldMemberIsNull{Value: true},
		}, nil
	}
	return types.SqlParameter{}, fmt.Errorf("rdsdata: unsupported driver.NamedValue type: %T", arg.Value)
}

func (d *DialectMySQL) IsIsolationLevelSupported(level sql.IsolationLevel) bool {
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

func (d *DialectMySQL) GetFieldConverter(columnType string) FieldConverter {
	// log.Printf("columnType: %s\n", columnType)
	switch strings.ToLower(columnType) {
	case "bigint unsigned":
		return func(field types.Field) (driver.Value, error) {
			v, ok := field.(*types.FieldMemberLongValue)
			if !ok {
				return nil, fmt.Errorf("rdsdata: unexpected field type %T", field)
			}
			// go-sql-driver/mysql converts BIGINT UNSIGNED to uint64.
			return uint64(v.Value), nil
		}
	case "decimal", "char", "varchar", "text", "enum":
		return func(field types.Field) (driver.Value, error) {
			v, ok := field.(*types.FieldMemberStringValue)
			if !ok {
				return nil, fmt.Errorf("rdsdata: unexpected field type %T", field)
			}
			// go-sql-driver/mysql converts these types to []byte.
			return []byte(v.Value), nil
		}
	case "float":
		return func(field types.Field) (driver.Value, error) {
			v, ok := field.(*types.FieldMemberDoubleValue)
			if !ok {
				return nil, fmt.Errorf("rdsdata: unexpected field type %T", field)
			}
			// go-sql-driver/mysql converts FLOAT to float32.
			return float32(v.Value), nil
		}
	}
	return convertDefault
}
