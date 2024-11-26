package rdsdata

import "database/sql/driver"

var _ driver.Driver = (*Driver)(nil)
var _ driver.DriverContext = (*Driver)(nil)

type Driver struct{}

func (d *Driver) Open(name string) (driver.Conn, error) {
	// TODO: implement
	return nil, nil
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	// TODO: implement
	return nil, nil
}
