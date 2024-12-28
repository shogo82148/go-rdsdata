package rdsdata

import (
	"context"
	"database/sql/driver"
)

var _ driver.Driver = (*Driver)(nil)
var _ driver.DriverContext = (*Driver)(nil)

// Driver is the RDS Data API driver.
type Driver struct{}

// NewDriver returns a new driver.
func NewDriver() *Driver {
	return &Driver{}
}

// Open opens a new connection to the database.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector returns a new connector.
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	connector := newConnector(d, cfg)
	return connector, nil
}
