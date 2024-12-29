package rdsdata

// compile time type check
var _ Dialect = (*DialectMySQL)(nil)

// DialectMySQL is the MySQL dialect.
type DialectMySQL struct{}
