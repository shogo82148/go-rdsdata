package rdsdata

import "database/sql/driver"

var _ driver.Driver = (*Driver)(nil)
var _ driver.DriverContext = (*Driver)(nil)

// Driver is the RDS Data API driver.
type Driver struct{}

// NewDriver returns a new driver.
func NewDriver() *Driver {
	return &Driver{}
}

// Open opens a new connection to the database.
func (d *Driver) Open(name string) (driver.Conn, error) {
	// TODO: implement
	return nil, nil
}

// OpenConnector returns a new connector.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	// TODO: implement
	return nil, nil
}
