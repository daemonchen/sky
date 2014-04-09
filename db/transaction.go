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
		return fmt.Errorf("dbi error: %s", err)
	}
	return nil
}

// get returns the value for a given key in a given named db.
func (t *transaction) get(name string, key []byte) ([]byte, error) {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return nil, fmt.Errorf("dbi error: %s", err)
	}

	value, err := t.Get(dbi, []byte(key))
	if err != nil && err != mdb.NotFound {
		return nil, fmt.Errorf("get error: %s", err)
	}
	return value, nil
}

// getAt returns a value within a multi-value key.
func (t *transaction) getAt(name string, key, prefix []byte) ([]byte, error) {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return nil, fmt.Errorf("dbi error: %s: %s", name, err)
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
		return nil, fmt.Errorf("getAt error: %s", mdb.Errno(ret))
	}
	value := C.GoBytes(v.mv_data, C.int(v.mv_size))
	if !bytes.HasPrefix(value, prefix) {
		return nil, nil
	}
	return value, nil
}

// getAll returns all values within a multi-value key.
func (t *transaction) getAll(name string, key []byte) ([][]byte, error) {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return nil, fmt.Errorf("dbi error: %s: ", name, err)
	}

	c, err := t.CursorOpen(dbi)
	if err != nil {
		return nil, fmt.Errorf("getAll cursor error: %s", err)
	}
	defer c.Close()

	// Move to the first value in the key.
	var buf = make([]byte, 1)
	var k = C.MDB_val{mv_size: C.size_t(len(key)), mv_data: unsafe.Pointer(&key[0])}
	var zeroValue = C.MDB_val{mv_size: 1, mv_data: unsafe.Pointer(&buf[0])}

	ret := C.mdb_cursor_get(c.MdbCursor(), &k, &zeroValue, C.MDB_cursor_op(mdb.GET_RANGE))
	if mdb.Errno(ret) == mdb.NotFound {
		return nil, nil
	} else if ret != mdb.SUCCESS {
		return nil, fmt.Errorf("getAll cursor_get error: %s", mdb.Errno(ret))
	}

	var values [][]byte
	for _, v, err := c.Get(key, mdb.GET_CURRENT); err != mdb.NotFound; _, v, err = c.Get(key, mdb.GET_CURRENT) {
		if err != nil {
			return nil, fmt.Errorf("getAll cursor iterate error: %s", err)
		}
		values = append(values, v)

		// Move cursor forward.
		if _, _, err := c.Get(key, mdb.NEXT_DUP); err == mdb.NotFound {
			break
		} else if err != nil {
			return nil, fmt.Errorf("next dup error: %s", err)
		}
	}

	return values, nil
}

// put sets the value for a given key in a given named db.
func (t *transaction) put(name string, key []byte, value []byte) error {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return fmt.Errorf("dbi error: %s", err)
	}

	if err := t.Put(dbi, []byte(key), value, mdb.NODUPDATA); err != nil {
		return fmt.Errorf("put error: %s", err)
	}
	return nil
}

// put sets the value for a value inside a dupsort key.
func (t *transaction) putAt(name string, key, prefix, value []byte) error {
	// First delete any existing value.
	if err := t.delAt(name, key, prefix); err != nil {
		return fmt.Errorf("putAt del error: %s", err)
	}

	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return fmt.Errorf("dbi error: %s", err)
	}

	if err := t.Put(dbi, []byte(key), value, mdb.NODUPDATA); err != nil {
		return fmt.Errorf("txn error on %s with %s=%s, prefix=%s: %s", name, key, string(value), prefix, err)
	}
	return nil
}

// del deletes a key.
func (t *transaction) del(name string, key []byte) error {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return fmt.Errorf("dbi error: %s", err)
	}
	if err := t.Del(dbi, []byte(key), nil); err != nil && err != mdb.NotFound {
		return fmt.Errorf("del error: %s", err)
	}
	return nil
}

// delAt deletes a value within a multi-value key.
func (t *transaction) delAt(name string, key, prefix []byte) error {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return fmt.Errorf("dbi error: %s", err)
	}

	c, err := t.CursorOpen(dbi)
	if err != nil {
		return fmt.Errorf("delAt cursor error: %s", err)
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
		return fmt.Errorf("delAt cursor_get error: %s", mdb.Errno(ret))
	}
	value := C.GoBytes(v.mv_data, C.int(v.mv_size))
	if !bytes.HasPrefix(value, prefix) {
		return nil
	}
	if err := c.Del(0); err != nil {
		return fmt.Errorf("delAt del error: %s", err)
	}
	return nil
}
