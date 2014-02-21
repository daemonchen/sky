package db

/*
#cgo LDFLAGS: -L/usr/local/lib -llmdb
#cgo CFLAGS: -I/usr/local/include

#include <stdlib.h>
#include <stdio.h>
#include <lmdb.h>
*/
import "C"

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/szferi/gomdb"
)

// transaction wraps an LMDB transaction into something more tolerable.
type transaction struct {
	*mdb.Txn
}

// dbi creates a db in the transaction with the given name and flags.
func (t *transaction) dbi(name string, flags uint) error {
	_, err := t.DBIOpen(&name, mdb.CREATE|flags)
	if err != nil {
		return &Error{"dbi error", err}
	}
	return nil
}

// get returns the value for a given key in a given named db.
func (t *transaction) get(name string, key []byte) ([]byte, error) {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return nil, &Error{"dbi error", err}
	}

	value, err := t.Get(dbi, []byte(key))
	if err != nil && err != mdb.NotFound {
		return nil, &Error{"get error", err}
	}
	return value, nil
}

// getAt returns a value within a multi-value key.
func (t *transaction) getAt(name string, key, prefix []byte) ([]byte, error) {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return nil, &Error{fmt.Sprintf("dbi error (%s)", name), err}
	}

	c, err := t.CursorOpen(dbi)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	// Read one of the values from the key.
	var k C.MDB_val
	var v C.MDB_val
	k.mv_size = C.size_t(len(key))
	k.mv_data = unsafe.Pointer(&key[0])
	v.mv_size = C.size_t(len(prefix))
	v.mv_data = unsafe.Pointer(&prefix[0])

	ret := C.mdb_cursor_get(c.MdbCursor(), &k, &v, C.MDB_cursor_op(mdb.GET_RANGE))
	if ret != mdb.SUCCESS {
		if mdb.Errno(ret) == mdb.NotFound {
			return nil, nil
		}
		return nil, mdb.Errno(ret)
	}
	value := C.GoBytes(v.mv_data, C.int(v.mv_size))
	if !bytes.HasPrefix(value, prefix) {
		return nil, nil
	}
	return value, nil
}

// put sets the value for a given key in a given named db.
func (t *transaction) put(name string, key []byte, value []byte) error {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return &Error{"dbi error", err}
	}

	if err := t.Put(dbi, []byte(key), value, mdb.NODUPDATA); err != nil {
		return &Error{"put error", err}
	}
	return nil
}

// put sets the value for a value inside a dupsort key.
func (t *transaction) putAt(name string, key, prefix, value []byte) error {
	// First delete any existing value.
	if err := t.delAt(name, key, prefix); err != nil {
		return err
	}

	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return &Error{"dbi error", err}
	}

	if err := t.Put(dbi, []byte(key), value, mdb.NODUPDATA); err != nil {
		return &Error{"putAt error", err}
	}
	return nil
}

// delAt deletes a value within a multi-value key.
func (t *transaction) delAt(name string, key, prefix []byte) error {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return &Error{"dbi error", err}
	}

	c, err := t.CursorOpen(dbi)
	if err != nil {
		return err
	}
	defer c.Close()

	// Move to the appropriate value.
	var k C.MDB_val
	var v C.MDB_val
	k.mv_size = C.size_t(len(key))
	k.mv_data = unsafe.Pointer(&key[0])
	v.mv_size = C.size_t(len(prefix))
	v.mv_data = unsafe.Pointer(&prefix[0])

	ret := C.mdb_cursor_get(c.MdbCursor(), &k, &v, C.MDB_cursor_op(mdb.GET_RANGE))
	if ret != mdb.SUCCESS {
		if mdb.Errno(ret) == mdb.NotFound {
			return nil
		}
		return mdb.Errno(ret)
	}
	value := C.GoBytes(k.mv_data, C.int(k.mv_size))
	if !bytes.HasPrefix(value, prefix) {
		return nil
	}
	return c.Del(0)
}
