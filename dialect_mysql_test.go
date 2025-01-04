package rdsdata

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

func TestDialectMySQL_MigrateQuery(t *testing.T) {
	t.Run("convert int64 parameter", func(t *testing.T) {
		d := &DialectMySQL{}
		input, err := d.MigrateQuery("SELECT ?", []driver.NamedValue{
			{
				Ordinal: 1,
				Value:   int64(42),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if v := aws.ToString(input.Sql); v != "SELECT :1" {
			t.Errorf("unexpected SQL: %q, want \"SELECT :1\"", v)
		}
		if len(input.Parameters) != 1 {
			t.Fatalf("unexpected number of parameters: %d, want 1", len(input.Parameters))
		}
		if v := aws.ToString(input.Parameters[0].Name); v != "1" {
			t.Errorf("unexpected parameter name: %q, want \"1\"", v)
		}
		if v, ok := input.Parameters[0].Value.(*types.FieldMemberLongValue); !ok || v.Value != 42 {
			t.Errorf("unexpected parameter value: %v, want 42", v)
		}
	})

	t.Run("convert float64 parameter", func(t *testing.T) {
		d := &DialectMySQL{}
		input, err := d.MigrateQuery("SELECT ?", []driver.NamedValue{
			{
				Ordinal: 1,
				Value:   float64(42.0),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if v := aws.ToString(input.Sql); v != "SELECT :1" {
			t.Errorf("unexpected SQL: %q, want \"SELECT :1\"", v)
		}
		if len(input.Parameters) != 1 {
			t.Fatalf("unexpected number of parameters: %d, want 1", len(input.Parameters))
		}
		if v := aws.ToString(input.Parameters[0].Name); v != "1" {
			t.Errorf("unexpected parameter name: %q, want \"1\"", v)
		}
		if v, ok := input.Parameters[0].Value.(*types.FieldMemberDoubleValue); !ok || v.Value != 42.0 {
			t.Errorf("unexpected parameter value: %v, want 42.0", v)
		}
	})

	t.Run("convert true parameter", func(t *testing.T) {
		d := &DialectMySQL{}
		input, err := d.MigrateQuery("SELECT ?", []driver.NamedValue{
			{
				Ordinal: 1,
				Value:   true,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if v := aws.ToString(input.Sql); v != "SELECT :1" {
			t.Errorf("unexpected SQL: %q, want \"SELECT :1\"", v)
		}
		if len(input.Parameters) != 1 {
			t.Fatalf("unexpected number of parameters: %d, want 1", len(input.Parameters))
		}
		if v := aws.ToString(input.Parameters[0].Name); v != "1" {
			t.Errorf("unexpected parameter name: %q, want \"1\"", v)
		}
		if v, ok := input.Parameters[0].Value.(*types.FieldMemberLongValue); !ok || v.Value != 1 {
			t.Errorf("unexpected parameter value: %v, want 1", v)
		}
	})

	t.Run("convert false parameter", func(t *testing.T) {
		d := &DialectMySQL{}
		input, err := d.MigrateQuery("SELECT ?", []driver.NamedValue{
			{
				Ordinal: 1,
				Value:   false,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if v := aws.ToString(input.Sql); v != "SELECT :1" {
			t.Errorf("unexpected SQL: %q, want \"SELECT :1\"", v)
		}
		if len(input.Parameters) != 1 {
			t.Fatalf("unexpected number of parameters: %d, want 1", len(input.Parameters))
		}
		if v := aws.ToString(input.Parameters[0].Name); v != "1" {
			t.Errorf("unexpected parameter name: %q, want \"1\"", v)
		}
		if v, ok := input.Parameters[0].Value.(*types.FieldMemberLongValue); !ok || v.Value != 0 {
			t.Errorf("unexpected parameter value: %v, want 0", v)
		}
	})

	t.Run("convert []byte parameter", func(t *testing.T) {
		d := &DialectMySQL{}
		input, err := d.MigrateQuery("SELECT ?", []driver.NamedValue{
			{
				Ordinal: 1,
				Value:   []byte("hello"),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if v := aws.ToString(input.Sql); v != "SELECT :1" {
			t.Errorf("unexpected SQL: %q, want \"SELECT :1\"", v)
		}
		if len(input.Parameters) != 1 {
			t.Fatalf("unexpected number of parameters: %d, want 1", len(input.Parameters))
		}
		if v := aws.ToString(input.Parameters[0].Name); v != "1" {
			t.Errorf("unexpected parameter name: %q, want \"1\"", v)
		}
		if v, ok := input.Parameters[0].Value.(*types.FieldMemberBlobValue); !ok || string(v.Value) != "hello" {
			t.Errorf("unexpected parameter value: %q, want \"hello\"", v.Value)
		}
	})

	t.Run("convert string parameter", func(t *testing.T) {
		d := &DialectMySQL{}
		input, err := d.MigrateQuery("SELECT ?", []driver.NamedValue{
			{
				Ordinal: 1,
				Value:   "hello",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if v := aws.ToString(input.Sql); v != "SELECT :1" {
			t.Errorf("unexpected SQL: %q, want \"SELECT :1\"", v)
		}
		if len(input.Parameters) != 1 {
			t.Fatalf("unexpected number of parameters: %d, want 1", len(input.Parameters))
		}
		if v := aws.ToString(input.Parameters[0].Name); v != "1" {
			t.Errorf("unexpected parameter name: %q, want \"1\"", v)
		}
		if v, ok := input.Parameters[0].Value.(*types.FieldMemberStringValue); !ok || string(v.Value) != "hello" {
			t.Errorf("unexpected parameter value: %q, want \"hello\"", v.Value)
		}
	})

	t.Run("convert time.Time parameter", func(t *testing.T) {
		d := &DialectMySQL{}
		input, err := d.MigrateQuery("SELECT ?", []driver.NamedValue{
			{
				Ordinal: 1,
				Value:   time.Date(2006, 1, 2, 15, 4, 5, 999_999_999, time.UTC),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if v := aws.ToString(input.Sql); v != "SELECT :1" {
			t.Errorf("unexpected SQL: %q, want \"SELECT :1\"", v)
		}
		if len(input.Parameters) != 1 {
			t.Fatalf("unexpected number of parameters: %d, want 1", len(input.Parameters))
		}
		if v := aws.ToString(input.Parameters[0].Name); v != "1" {
			t.Errorf("unexpected parameter name: %q, want \"1\"", v)
		}
		if v, ok := input.Parameters[0].Value.(*types.FieldMemberStringValue); !ok || string(v.Value) != "2006-01-02 15:04:05.999999999" {
			t.Errorf("unexpected parameter value: %q, want 2006-01-02 15:04:05.999999999", v.Value)
		}
	})

	t.Run("convert nil parameter", func(t *testing.T) {
		d := &DialectMySQL{}
		input, err := d.MigrateQuery("SELECT ?", []driver.NamedValue{
			{
				Ordinal: 1,
				Value:   nil,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if v := aws.ToString(input.Sql); v != "SELECT :1" {
			t.Errorf("unexpected SQL: %q, want \"SELECT :1\"", v)
		}
		if len(input.Parameters) != 1 {
			t.Fatalf("unexpected number of parameters: %d, want 1", len(input.Parameters))
		}
		if v := aws.ToString(input.Parameters[0].Name); v != "1" {
			t.Errorf("unexpected parameter name: %q, want \"1\"", v)
		}
		if v, ok := input.Parameters[0].Value.(*types.FieldMemberIsNull); !ok || !v.Value {
			t.Errorf("unexpected parameter value: %v, want true", v.Value)
		}
	})
}

func TestDialectMySQL_GetFieldConverter(t *testing.T) {
	t.Run("BIGINT UNSIGNED", func(t *testing.T) {
		d := &DialectMySQL{}
		conv := d.GetFieldConverter("BIGINT UNSIGNED")
		v, err := conv(&types.FieldMemberLongValue{Value: 42})
		if err != nil {
			t.Fatal(err)
		}
		if v != uint64(42) {
			t.Errorf("unexpected value: %v, want 42", v)
		}
	})

	t.Run("BIGINT UNSIGNED NULL", func(t *testing.T) {
		d := &DialectMySQL{}
		conv := d.GetFieldConverter("BIGINT UNSIGNED")
		v, err := conv(&types.FieldMemberIsNull{Value: true})
		if err != nil {
			t.Fatal(err)
		}
		if v != nil {
			t.Errorf("unexpected value: %v, want nil", v)
		}
	})

	t.Run("FLOAT", func(t *testing.T) {
		d := &DialectMySQL{}
		conv := d.GetFieldConverter("FLOAT")
		v, err := conv(&types.FieldMemberDoubleValue{Value: 42.0})
		if err != nil {
			t.Fatal(err)
		}
		if v != float32(42.0) {
			t.Errorf("unexpected value: %v, want 42.0", v)
		}
	})

	t.Run("FLOAT NULL", func(t *testing.T) {
		d := &DialectMySQL{}
		conv := d.GetFieldConverter("FLOAT")
		v, err := conv(&types.FieldMemberIsNull{Value: true})
		if err != nil {
			t.Fatal(err)
		}
		if v != nil {
			t.Errorf("unexpected value: %v, want nil", v)
		}
	})

	t.Run("DATETIME parseTime=false", func(t *testing.T) {
		d := &DialectMySQL{
			parseTime: false,
		}
		conv := d.GetFieldConverter("DATETIME")
		v, err := conv(&types.FieldMemberStringValue{Value: "2006-01-02 15:04:05.999999999"})
		if err != nil {
			t.Fatal(err)
		}
		data, ok := v.([]byte)
		if !ok {
			t.Fatalf("unexpected value: %v, want []byte", v)
		}
		if string(data) != "2006-01-02 15:04:05.999999999" {
			t.Errorf("unexpected value: %v, want 2006-01-02 15:04:05.999999999", v)
		}
	})

	t.Run("DATETIME parseTime=true", func(t *testing.T) {
		d := &DialectMySQL{
			parseTime: true,
		}
		conv := d.GetFieldConverter("DATETIME")
		v, err := conv(&types.FieldMemberStringValue{Value: "2006-01-02 15:04:05.999999999"})
		if err != nil {
			t.Fatal(err)
		}
		if v != time.Date(2006, 1, 2, 15, 4, 5, 999_999_999, time.UTC) {
			t.Errorf("unexpected value: %v, want 2006-01-02 15:04:05.999999999", v)
		}
	})

	t.Run("DATETIME NULL", func(t *testing.T) {
		d := &DialectMySQL{}
		conv := d.GetFieldConverter("DATETIME")
		v, err := conv(&types.FieldMemberIsNull{Value: true})
		if err != nil {
			t.Fatal(err)
		}
		if v != nil {
			t.Errorf("unexpected value: %v, want nil", v)
		}
	})

	t.Run("FieldMemberLongValue", func(t *testing.T) {
		d := &DialectMySQL{}
		conv := d.GetFieldConverter("BIGINT")
		v, err := conv(&types.FieldMemberLongValue{Value: 42})
		if err != nil {
			t.Fatal(err)
		}
		if v != int64(42) {
			t.Errorf("unexpected value: %v, want 42", v)
		}
	})

	t.Run("FieldMemberDoubleValue", func(t *testing.T) {
		d := &DialectMySQL{}
		conv := d.GetFieldConverter("DOUBLE")
		v, err := conv(&types.FieldMemberDoubleValue{Value: 42.0})
		if err != nil {
			t.Fatal(err)
		}
		if v != float64(42.0) {
			t.Errorf("unexpected value: %v, want 42.0", v)
		}
	})

	t.Run("FieldMemberBooleanValue", func(t *testing.T) {
		d := &DialectMySQL{}
		conv := d.GetFieldConverter("BOOLEAN")
		v, err := conv(&types.FieldMemberBooleanValue{Value: true})
		if err != nil {
			t.Fatal(err)
		}
		if v != int64(1) {
			t.Errorf("unexpected value: %v, want 1", v)
		}

		v, err = conv(&types.FieldMemberBooleanValue{Value: false})
		if err != nil {
			t.Fatal(err)
		}
		if v != int64(0) {
			t.Errorf("unexpected value: %v, want 0", v)
		}
	})

	t.Run("FieldMemberBlobValue", func(t *testing.T) {
		d := &DialectMySQL{}
		conv := d.GetFieldConverter("BLOB")
		v, err := conv(&types.FieldMemberBlobValue{Value: []byte("hello")})
		if err != nil {
			t.Fatal(err)
		}
		if string(v.([]byte)) != "hello" {
			t.Errorf("unexpected value: %v, want hello", v)
		}
	})

	t.Run("FieldMemberStringValue", func(t *testing.T) {
		d := &DialectMySQL{}
		conv := d.GetFieldConverter("VARCHAR")
		v, err := conv(&types.FieldMemberStringValue{Value: "hello"})
		if err != nil {
			t.Fatal(err)
		}
		if string(v.([]byte)) != "hello" {
			t.Errorf("unexpected value: %v, want hello", v)
		}
	})

	t.Run("FieldMemberIsNull", func(t *testing.T) {
		d := &DialectMySQL{}
		conv := d.GetFieldConverter("VARCHAR")
		v, err := conv(&types.FieldMemberIsNull{})
		if err != nil {
			t.Fatal(err)
		}
		if v != nil {
			t.Errorf("unexpected value: %v, want nil", v)
		}
	})
}
