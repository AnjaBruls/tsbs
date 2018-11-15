package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/SiriDB/go-siridb-connector"
)

type dbCreator struct {
	conn1 *siridb.Connection
	conn2 *siridb.Connection
}

// Init should set up any connection or other setup for talking to the DB, but should NOT create any databases
func (d *dbCreator) Init() {
	d.conn1 = siridb.NewConnection("localhost", 9000) // SHOULD BE VARIABLES
	d.conn2 = siridb.NewConnection("localhost", 9001) // SHOULD BE VARIABLES
}

// DBExists checks if a database with the given name currently exists.
func (d *dbCreator) DBExists(dbName string) bool {
	if err := d.conn1.Connect(dbUser, dbPass, dbName); err == nil {
		return true
	}
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {

	paths := strings.Split(dbpaths, ",")
	for _, dbpath := range paths {
		if err := os.RemoveAll(fmt.Sprintf("%s/%s", dbpath, dbName)); err != nil {
			return err
		}
	}
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

	if _, err := d.conn1.Manage(account, password, siridb.AdminNewDatabase, options1); err != nil {
		return err
	}

	options2 := make(map[string]interface{})

	options2["dbname"] = dbName
	options2["host"] = "localhost"
	options2["port"] = 9000
	options2["username"] = dbUser
	options2["password"] = dbPass
	// options2["pool"] = 0

	if _, err := d.conn2.Manage(account, password, siridb.AdminNewPool, options2); err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

// RemoveOldDB removes an existing database with the given name.
// func (d *dbCreator) RemoveOldDB(dbName string) error {

// }

func (d *dbCreator) Close() {
	fmt.Println("close")
	d.conn1.Close()
	d.conn2.Close()
}
