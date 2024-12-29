package rdsdata

import (
	"database/sql"
)

// compile time type check
var _ Dialect = (*DialectMySQL)(nil)

// DialectMySQL is the MySQL dialect.
type DialectMySQL struct{}

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
	return convertDefault
}
