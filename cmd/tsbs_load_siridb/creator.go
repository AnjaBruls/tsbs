package main

import (
	"fmt"
	"strconv"
	"strings"

	"../../../go-siridb-connector"
)

type dbCreator struct {
	connection map[string]*siridb.Connection
	hostlist   []string
}

// Init should set up any connection or other setup for talking to the DB, but should NOT create any databases
func (d *dbCreator) Init() {

	d.hostlist = strings.Split(hosts, ",")
	d.connection = make(map[string]*siridb.Connection)
	for _, hostport := range d.hostlist {
		host_port := strings.Split(hostport, ":")
		host := host_port[0]
		port, err := strconv.ParseUint(host_port[1], 10, 16)
		if err != nil {
			fatal(err)
		}
		d.connection[hostport] = siridb.NewConnection(host, uint16(port))
	}
}

// DBExists checks if a database with the given name currently exists.
func (d *dbCreator) DBExists(dbName string) bool {
	for _, host := range d.hostlist {
		if err := d.connection[host].Connect(dbUser, dbPass, dbName); err == nil {
			return true
		}
	}
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {

	// paths := strings.Split(dbpaths, ",")
	// for _, dbpath := range paths {
	// if err := os.RemoveAll(fmt.Sprintf("%s/%s", dbpath, dbName)); err != nil {
	// 	return err
	// }
	// }
	fatal("Cannot remove database")
	return nil
}

// CreateDB creates a database with the given name.
func (d *dbCreator) CreateDB(dbName string) error {

	options1 := make(map[string]interface{})
	options1["dbname"] = dbName
	options1["time_precision"] = timePrecision
	options1["buffer_size"] = bufferSize
	options1["duration_num"] = durationNum
	options1["duration_log"] = durationLog

	if _, err := d.connection[d.hostlist[0]].Manage(account, password, siridb.AdminNewDatabase, options1); err != nil {
		return err
	}

	if createNewPool && len(d.hostlist) > 1 {
		options2 := make(map[string]interface{})
		options2["dbname"] = dbName
		options2["host"] = "localhost"
		options2["port"] = 9000
		options2["username"] = dbUser
		options2["password"] = dbPass

		if _, err := d.connection[d.hostlist[1]].Manage(account, password, siridb.AdminNewPool, options2); err != nil {
			return err
		}
	}

	if createReplica && len(d.hostlist) > 1 {
		options2 := make(map[string]interface{})
		options2["dbname"] = dbName
		options2["host"] = "localhost"
		options2["port"] = 9000
		options2["username"] = dbUser
		options2["password"] = dbPass
		options2["pool"] = 0

		if _, err := d.connection[d.hostlist[1]].Manage(account, password, siridb.AdminNewReplica, options2); err != nil {
			return err
		}
	}

	return nil
}

// RemoveOldDB removes an existing database with the given name.
// func (d *dbCreator) RemoveOldDB(dbName string) error {

// }

func (d *dbCreator) Close() {
	fmt.Println("close")
	for _, conn := range d.connection {
		conn.Close()
	}
}
