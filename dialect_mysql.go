package rdsdata

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

var ordinalRegex = regexp.MustCompile(`\?`)

// compile time type check
var _ Dialect = (*DialectMySQL)(nil)

// DialectMySQL is the MySQL dialect.
type DialectMySQL struct {
	location     *time.Location
	parseTime    bool
	timeTruncate time.Duration
}

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
		const format = "2006-01-02 15:04:05.999999999"
		t := v.In(d.getLocation()).Truncate(d.timeTruncate)
		return types.SqlParameter{
			Name:  &name,
			Value: &types.FieldMemberStringValue{Value: t.Format(format)},
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

func (d *DialectMySQL) getLocation() *time.Location {
	if d.location == nil {
		return time.UTC
	}
	return d.location
}

func (d *DialectMySQL) GetFieldConverter(columnType string) FieldConverter {
	// log.Printf("columnType: %s\n", columnType)
	switch columnType {
	case "BIGINT UNSIGNED":
		return func(field types.Field) (driver.Value, error) {
			switch v := field.(type) {
			case *types.FieldMemberLongValue:
				// go-sql-driver/mysql converts BIGINT UNSIGNED to uint64.
				return uint64(v.Value), nil
			case *types.FieldMemberIsNull:
				return nil, nil
			default:
				return nil, fmt.Errorf("rdsdata: unsupported field type: %T", v)
			}
		}
	case "FLOAT":
		return func(field types.Field) (driver.Value, error) {
			switch v := field.(type) {
			case *types.FieldMemberDoubleValue:
				// go-sql-driver/mysql converts FLOAT to float32.
				return float32(v.Value), nil
			case *types.FieldMemberIsNull:
				return nil, nil
			default:
				return nil, fmt.Errorf("rdsdata: unsupported field type: %T", v)
			}
		}
	}
	return convertMySQLDefault
}

func convertMySQLDefault(field types.Field) (driver.Value, error) {
	switch v := field.(type) {
	case *types.FieldMemberLongValue:
		return v.Value, nil
	case *types.FieldMemberDoubleValue:
		return v.Value, nil
	case *types.FieldMemberBooleanValue:
		if v.Value {
			return int64(1), nil
		} else {
			return int64(0), nil
		}
	case *types.FieldMemberBlobValue:
		return v.Value, nil
	case *types.FieldMemberStringValue:
		// go-sql-driver/mysql converts string to []byte.
		return []byte(v.Value), nil
	case *types.FieldMemberArrayValue:
		return v.Value, nil
	case *types.FieldMemberIsNull:
		return nil, nil
	default:
		return nil, fmt.Errorf("rdsdata: unsupported field type: %T", v)
	}
}
