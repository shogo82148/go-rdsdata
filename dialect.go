package rdsdata

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

// FieldConverter is a function that converts the passed result row field into the expected type.
type FieldConverter func(field types.Field) (driver.Value, error)

type Dialect interface {
	IsIsolationLevelSupported(level sql.IsolationLevel) bool
	GetFieldConverter(columnType string) FieldConverter
}

func convertNamedValues(args []driver.NamedValue) ([]types.SqlParameter, error) {
	params := make([]types.SqlParameter, len(args))
	for i, arg := range args {
		sqlParam, err := convertNamedValue(arg)
		if err != nil {
			return nil, err
		}
		params[i] = sqlParam
	}
	return params, nil
}

func convertNamedValue(arg driver.NamedValue) (types.SqlParameter, error) {
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
		return types.SqlParameter{
			Name:  &name,
			Value: &types.FieldMemberBooleanValue{Value: v},
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

func convertDefault(field types.Field) (driver.Value, error) {
	switch v := field.(type) {
	case *types.FieldMemberLongValue:
		return v.Value, nil
	case *types.FieldMemberDoubleValue:
		return v.Value, nil
	case *types.FieldMemberBooleanValue:
		return v.Value, nil
	case *types.FieldMemberBlobValue:
		return v.Value, nil
	case *types.FieldMemberStringValue:
		return v.Value, nil
	case *types.FieldMemberArrayValue:
		return v.Value, nil
	case *types.FieldMemberIsNull:
		return nil, nil
	default:
		return nil, fmt.Errorf("rdsdata: unsupported field type: %T", v)
	}
}
