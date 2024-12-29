package rdsdata

import "database/sql"

type Dialect interface {
	IsIsolationLevelSupported(level sql.IsolationLevel) bool
}
