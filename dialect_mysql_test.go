package rdsdata

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

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
