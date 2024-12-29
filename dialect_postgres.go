package rdsdata

// compile time type check
var _ Dialect = (*DialectPostgres)(nil)

// DialectPostgres is the PostgreSQL dialect.
type DialectPostgres struct{}
