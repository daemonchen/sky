package mapper

import (
	"github.com/axw/gollvm/llvm"
)

const (
	MDB_FIRST          = iota /**< Position at first key/data item */
	MDB_FIRST_DUP             /**< Position at first data item of current key. Only for #MDB_DUPSORT */
	MDB_GET_BOTH              /**< Position at key/data pair. Only for #MDB_DUPSORT */
	MDB_GET_BOTH_RANGE        /**< position at key, nearest data. Only for #MDB_DUPSORT */
	MDB_GET_CURRENT           /**< Return key/data at current cursor position */
	MDB_GET_MULTIPLE          /**< Return all the duplicate data items at the current cursor position. Only for #MDB_DUPFIXED */
	MDB_LAST                  /**< Position at last key/data item */
	MDB_LAST_DUP              /**< Position at last data item of current key. Only for #MDB_DUPSORT */
	MDB_NEXT                  /**< Position at next data item */
	MDB_NEXT_DUP              /**< Position at next data item of current key. Only for #MDB_DUPSORT */
	MDB_NEXT_MULTIPLE         /**< Return all duplicate data items at the next cursor position. Only for #MDB_DUPFIXED */
	MDB_NEXT_NODUP            /**< Position at first data item of next key */
	MDB_PREV                  /**< Position at previous data item */
	MDB_PREV_DUP              /**< Position at previous data item of current key. Only for #MDB_DUPSORT */
	MDB_PREV_NODUP            /**< Position at last data item of previous key */
	MDB_SET                   /**< Position at specified key */
	MDB_SET_KEY               /**< Position at specified key, return key + data */
	MDB_SET_RANGE             /**< Position at first key greater than or equal to specified key. */
)

// [codegen]
// typedef struct MDB_cursor;
// typedef struct MDB_val;
// int64_t mdb_cursor_get(MDB_cursor*, MDB_val*, MDB_val*, int64_t);
func (m *Mapper) declare_lmdb() {
	m.mdbCursorType = m.context.StructCreateNamed("MDB_cursor")
	m.mdbValType = m.context.StructCreateNamed("MDB_val")
	m.mdbValType.StructSetBody([]llvm.Type{
		m.context.Int64Type(),                     // size_t mv_size
		llvm.PointerType(m.context.Int8Type(), 0), // void *mv_data
	}, false)

	llvm.AddFunction(m.module, "mdb_cursor_get", llvm.FunctionType(m.context.Int64Type(), []llvm.Type{llvm.PointerType(m.mdbCursorType, 0), llvm.PointerType(m.mdbValType, 0), llvm.PointerType(m.mdbValType, 0), m.context.Int64Type()}, false))
}
