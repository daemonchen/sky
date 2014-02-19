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
	"github.com/szferi/gomdb"
	"unsafe"
)

func mdbGet2(cursor *mdb.Cursor, setKey []byte, setVal []byte, op uint) (key, val []byte, err error) {
	var ckey C.MDB_val
	var cval C.MDB_val
	if setKey != nil && (op == mdb.GET_BOTH || op == mdb.GET_RANGE || op == mdb.SET || op == mdb.SET_KEY || op == mdb.SET_RANGE) {
		ckey.mv_size = C.size_t(len(setKey))
		ckey.mv_data = unsafe.Pointer(&setKey[0])
	}
	if setVal != nil && (op == mdb.GET_BOTH || op == mdb.GET_RANGE) {
		cval.mv_size = C.size_t(len(setVal))
		cval.mv_data = unsafe.Pointer(&setVal[0])
	}
	ret := C.mdb_cursor_get(cursor.MdbCursor(), &ckey, &cval, C.MDB_cursor_op(op))
	if ret != mdb.SUCCESS {
		err = mdb.Errno(ret)
		key = nil
		val = nil
		return
	}
	err = nil
	key = C.GoBytes(ckey.mv_data, C.int(ckey.mv_size))
	val = C.GoBytes(cval.mv_data, C.int(cval.mv_size))
	return
}
