// Copyright 2018-2020 Petr Homola. All rights reserved.
// Use of this source code is governed by the AGPL v3.0
// that can be found in the LICENSE file.

// A cgo-based wrapper around SQLite.
package sqlite

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"unsafe"
)

/*
#include <stdlib.h>
#include <sqlite3.h>
inline sqlite3_destructor_type sqlite3_const_transient() { return SQLITE_TRANSIENT; }
inline sqlite3_destructor_type sqlite3_const_static() { return SQLITE_STATIC; }
inline char* sqlite3_charptr(unsigned char* s) { return (void*)s; }
#cgo LDFLAGS: -lsqlite3
*/
import "C"

// A database instance.
type Database struct {
	db   *C.sqlite3
	lock sync.Mutex
}

// Returns a new database.
func NewDatabase(path string) (*Database, error) {
	var db *C.sqlite3
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	s := C.sqlite3_open(p, &db)
	if s == C.SQLITE_OK {
		return &Database{db: db}, nil
	} else {
		return nil, fmt.Errorf("couldn't open database file (%s)", path)
	}
}

// Activates the associated lock.
func (db *Database) Lock() {
	db.lock.Lock()
}

// Deactivates the associated lock.
func (db *Database) Unlock() {
	db.lock.Unlock()
}

// Closes the database.
func (db *Database) Close() {
	C.sqlite3_close(db.db)
	log.Print("database closed")
}

// Executes an SQL statement.
func (db *Database) Execute(sql string) error {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var err *C.char
	s := C.sqlite3_exec(db.db, cs, nil, nil, &err)
	if s != C.SQLITE_OK {
		return errors.New(C.GoString(err))
	}
	return nil
}

// An SQL statement.
type Statement struct {
	stmt *C.sqlite3_stmt
	db   *Database
}

// Returns a new statement.
func (db *Database) NewStatement(sql string) (*Statement, error) {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var stmt *C.sqlite3_stmt
	s := C.sqlite3_prepare(db.db, cs, -1, &stmt, nil)
	if s != C.SQLITE_OK {
		return nil, errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
	}
	return &Statement{stmt, db}, nil
}

// Closes the statement.
func (stmt *Statement) Close() {
	C.sqlite3_finalize(stmt.stmt)
}

// Moves on to the next row.
func (stmt *Statement) Step() error {
	s := C.sqlite3_step(stmt.stmt)
	if s != C.SQLITE_DONE {
		return errors.New(C.GoString(C.sqlite3_errmsg(stmt.db.db)))
	}
	return nil
}

// Enumerates all rows using the provided callback.
func (stmt *Statement) StepRows(cb func()) error {
	for {
		s := C.sqlite3_step(stmt.stmt)
		if s == C.SQLITE_ROW {
			cb()
		} else {
			if s != C.SQLITE_DONE {
				return errors.New("stepping through rows didn't finish with DONE")
			}
			return nil
		}
	}
}

// Returns the i-th column as int.
func (stmt *Statement) ColumnInt(i int) int {
	return int(C.sqlite3_column_int(stmt.stmt, C.int(i)))
}

// Returns the i-th column as int64.
func (stmt *Statement) ColumnInt64(i int) int64 {
	return int64(C.sqlite3_column_int64(stmt.stmt, C.int(i)))
}

// Returns the i-th column as double.
func (stmt *Statement) ColumnDouble(i int) float64 {
	return float64(C.sqlite3_column_double(stmt.stmt, C.int(i)))
}

// Returns the i-th column as string.
func (stmt *Statement) ColumnText(i int) string {
	cs := C.sqlite3_column_text(stmt.stmt, C.int(i))
	return C.GoString(C.sqlite3_charptr(cs))
}

// Returns the i-th column as blob.
func (stmt *Statement) ColumnBlob(i int) []byte {
	p := C.sqlite3_column_blob(stmt.stmt, C.int(i))
	len := C.sqlite3_column_bytes(stmt.stmt, C.int(i))
	return C.GoBytes(p, len)
}

// Binds the i-th column as int.
func (stmt *Statement) BindInt(i int, val int) {
	C.sqlite3_bind_int(stmt.stmt, C.int(i), C.int(val))
}

// Binds the i-th column as int64.
func (stmt *Statement) BindInt64(i int, val int64) {
	C.sqlite3_bind_int64(stmt.stmt, C.int(i), C.sqlite3_int64(val))
}

// Binds the i-th column as double.
func (stmt *Statement) BindDouble(i int, val float64) {
	C.sqlite3_bind_double(stmt.stmt, C.int(i), C.double(val))
}

// Binds the i-th column as string.
func (stmt *Statement) BindText(i int, val string) {
	s := C.CString(val)
	defer C.free(unsafe.Pointer(s))
	C.sqlite3_bind_text(stmt.stmt, C.int(i), s, -1, C.sqlite3_const_transient())
}

// Binds the i-th column as blob.
func (stmt *Statement) BindBlob(i int, b []byte) {
	p := C.CBytes(b)
	defer C.free(p)
	C.sqlite3_bind_blob(stmt.stmt, C.int(i), p, C.int(len(b)), C.sqlite3_const_transient())
}
