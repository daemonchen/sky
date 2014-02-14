package mapper

import (
	"github.com/axw/gollvm/llvm"
)

const (
	cursorEventElementIndex           = 0
	cursorNextEventElementIndex       = 1
	cursorLMDBCursorElementIndex      = 2
	cursorSessionIdleTimeElementIndex = 3
)

// [codegen]
// typedef struct {
//     sky_event *event;
//     sky_event *next_event;
//     MDB_cursor *lmdb_cursor;
// } sky_cursor;
func (m *Mapper) codegenCursorType() llvm.Type {
	typ := m.context.StructCreateNamed("sky_cursor")
	typ.StructSetBody([]llvm.Type{
		llvm.PointerType(m.eventType, 0),
		llvm.PointerType(m.eventType, 0),
		llvm.PointerType(m.mdbCursorType, 0),
		m.context.Int64Type(),
	}, false)
	return typ
}

// [codegen]
// bool sky_cursor_init(sky_cursor *)
// bool sky_cursor_next_object(sky_cursor *)
func (m *Mapper) codegenCursorExternalDecl() {
	llvm.AddFunction(m.module, "sky_cursor_next_object", llvm.FunctionType(m.ptrtype(), []llvm.Type{llvm.PointerType(m.cursorType, 0), m.context.Int64Type()}, false))
}
