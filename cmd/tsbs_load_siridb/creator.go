package main

import (
	"errors"
	"strconv"
	"strings"

	siridb "github.com/SiriDB/go-siridb-connector"
)

type dbCreator struct {
	connection []*siridb.Connection
	hosts      []string
	ports      []string
}

// Init should set up any connection or other setup for talking to the DB, but should NOT create any databases
func (d *dbCreator) Init() {

	d.hosts = strings.Split(host, ",")
	d.ports = strings.Split(port, ",")
	d.connection = make([]*siridb.Connection, 0)
	for i := range d.ports {
		portInt64, err := strconv.ParseUint(d.ports[i], 10, 16)
		if err != nil {
			fatal(err)
		}
		d.connection = append(d.connection, siridb.NewConnection(d.hosts[i], uint16(portInt64)))
	}
}

// DBExists checks if a database with the given name currently exists.
func (d *dbCreator) DBExists(dbName string) bool {
	for _, conn := range d.connection {
		if err := conn.Connect(dbUser, dbPass, dbName); err == nil {
			return true
		}
	}
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	msg := errors.New("database cannot be dropped, you need to stop the server and remove the database directory in your DBPATH")
	return msg
}

// CreateDB creates a database with the given name.
func (d *dbCreator) CreateDB(dbName string) error {
	defer d.Close()
	options1 := make(map[string]interface{})
	options1["dbname"] = dbName
	options1["time_precision"] = timePrecision
	options1["buffer_size"] = bufferSize
	options1["duration_num"] = durationNum
	options1["duration_log"] = durationLog

	if _, err := d.connection[0].Manage(account, password, siridb.AdminNewDatabase, options1); err != nil {
		return err
	}

	for i := 1; createNewPool && len(d.connection) > 1 && i < len(d.connection); i++ {
		options2 := make(map[string]interface{})
		options2["dbname"] = dbName
		options2["host"] = d.hosts[0]
		options2["port"] = d.ports[0]
		options2["username"] = dbUser
		options2["password"] = dbPass

		if _, err := d.connection[i].Manage(account, password, siridb.AdminNewPool, options2); err != nil {
			return err
		}
	}

	// if createReplica && len(d.connection) > 1 {
	// 	options2 := make(map[string]interface{})
	// 	options2["dbname"] = dbName
	// 	options2["host"] = d.hosts[0]
	// 	options2["port"] = d.ports[0]
	// 	options2["username"] = dbUser
	// 	options2["password"] = dbPass
	// 	options2["pool"] = 0

	// 	if _, err := d.connection[1].Manage(account, password, siridb.AdminNewReplica, options2); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func (d *dbCreator) Close() {
	for _, conn := range d.connection {
		conn.Close()
	}
}
