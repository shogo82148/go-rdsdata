package rdsdata

import "database/sql"

// compile time type check
var _ Dialect = (*DialectPostgres)(nil)

// DialectPostgres is the PostgreSQL dialect.
type DialectPostgres struct{}

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
