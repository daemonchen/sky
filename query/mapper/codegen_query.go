package mapper

import (
	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/hashmap"
)

func (m *Mapper) codegenQuery(q *ast.Query) (llvm.Value, error) {
	tbl := ast.NewSymtable(nil)

	if _, err := m.codegenVarDecls(q.VarDecls(), tbl); err != nil {
		return nilValue, err
	}

	llvm.AddFunction(m.module, "llvm.bswap.i64", llvm.FunctionType(m.context.Int64Type(), []llvm.Type{m.context.Int64Type()}, false))

	m.declare_lmdb()

	m.eventType = m.codegenEventType()
	m.cursorType = m.codegenCursorType()

	m.hashmapType = hashmap.DeclareType(m.module, m.context)
	hashmap.Declare(m.module, m.context, m.hashmapType)

	llvm.AddFunction(m.module, "debug", llvm.FunctionType(m.context.VoidType(), []llvm.Type{llvm.PointerType(m.context.Int8Type(), 0)}, false))
	m.codegenCursorExternalDecl()

	llvm.AddFunction(m.module, "printf", llvm.FunctionType(m.context.Int32Type(), []llvm.Type{}, true))

	Declare_unpack_int(m.module, m.context)
	Declare_unpack_double(m.module, m.context)
	Declare_unpack_bool(m.module, m.context)
	Declare_unpack_raw(m.module, m.context)
	Declare_unpack_map(m.module, m.context)
	Declare_sizeof_elem_and_data(m.module, m.context)

	m.codegenCursorNextEventFunc()

	// Generate the entry function.
	return m.codegenQueryEntryFunc(q, tbl)
}

// [codegen]
// int32_t entry(sky_cursor *cursor, sky_map *result) {
//     void *ptr;
//     cursor->event = malloc();
//     cursor->next_event = malloc();
//     ptr = cursor_next_object(cursor, true);
//     if(rc) goto loop_buffer_event else goto exit;
//
// loop:
//     ptr = cursor_next_object(cursor, false);
//     if(rc) goto loop_buffer_event else goto exit;
//
// loop_buffer_event:
//     rc = cursor_read_event(ptr);
//     if(rc == 0) goto loop_next_event else goto exit;
//
// loop_next_event:
//     rc = cursor_next_event(cursor);
//     if(rc == 0) goto loop_body else goto exit;
//
// loop_body:
//     ...generate...
//     goto loop;
//
// exit:
//     return;
// }
func (m *Mapper) codegenQueryEntryFunc(q *ast.Query, tbl *ast.Symtable) (llvm.Value, error) {
	sig := llvm.FunctionType(m.context.VoidType(), []llvm.Type{llvm.PointerType(m.cursorType, 0), llvm.PointerType(m.hashmapType, 0)}, false)
	fn := llvm.AddFunction(m.module, "entry", sig)
	fn.SetFunctionCallConv(llvm.CCallConv)

	// Generate functions for child statements.
	var statementFns []llvm.Value
	for _, statement := range q.Statements {
		statementFn, err := m.codegenStatement(statement, tbl)
		if err != nil {
			return nilValue, err
		}
		statementFns = append(statementFns, statementFn)
	}

	entry := m.context.AddBasicBlock(fn, "entry")
	loop := m.context.AddBasicBlock(fn, "loop")
	loop_buffer_event := m.context.AddBasicBlock(fn, "loop_buffer_event")
	loop_next_event := m.context.AddBasicBlock(fn, "loop_next_event")
	loop_body := m.context.AddBasicBlock(fn, "loop_body")
	exit := m.context.AddBasicBlock(fn, "exit")

	m.builder.SetInsertPointAtEnd(entry)
	m.trace("entry()")
	cursor_ref := m.alloca(llvm.PointerType(m.cursorType, 0), "cursor")
	result := m.alloca(llvm.PointerType(m.hashmapType, 0), "result")
	ptr := m.alloca(m.ptrtype(), "ptr")
	m.store(fn.Param(0), cursor_ref)
	m.store(fn.Param(1), result)
	event_ref := m.event_ref(cursor_ref)
	next_event_ref := m.next_event_ref(cursor_ref)

	m.store(m.builder.CreateMalloc(m.eventType, ""), event_ref)
	m.store(m.builder.CreateMalloc(m.eventType, ""), next_event_ref)

	m.store(m.call("sky_cursor_next_object", m.load(cursor_ref), m.constint(1)), ptr)
	m.condbr(m.icmp(llvm.IntNE, m.load(ptr), m.ptrnull()), loop_buffer_event, exit)

	m.builder.SetInsertPointAtEnd(loop)
	m.store(m.call("sky_cursor_next_object", m.load(cursor_ref), m.constint(0)), ptr)
	m.condbr(m.icmp(llvm.IntNE, m.load(ptr), m.ptrnull()), loop_buffer_event, exit)

	m.builder.SetInsertPointAtEnd(loop_buffer_event)
	m.call("sky_event_reset", m.load(event_ref))
	m.call("sky_event_reset", m.load(next_event_ref))
	rc := m.call("cursor_read_event", m.load(m.structgep(m.load(cursor_ref, ""), cursorNextEventElementIndex)), m.load(ptr))
	m.condbr(m.icmp(llvm.IntEQ, rc, m.constint(0)), loop_next_event, exit)

	m.builder.SetInsertPointAtEnd(loop_next_event)
	rc = m.call("cursor_next_event", m.load(cursor_ref, ""))
	m.condbr(m.icmp(llvm.IntEQ, rc, m.constint(0)), loop_body, exit)

	m.builder.SetInsertPointAtEnd(loop_body)
	for _, statementFn := range statementFns {
		m.builder.CreateCall(statementFn, []llvm.Value{m.load(cursor_ref, ""), m.load(result, "")}, "")
	}
	m.br(loop)

	m.builder.SetInsertPointAtEnd(exit)
	m.builder.CreateFree(m.load(m.structgep(m.load(cursor_ref, ""), cursorEventElementIndex, ""), ""))
	m.builder.CreateFree(m.load(m.structgep(m.load(cursor_ref, ""), cursorNextEventElementIndex, ""), ""))
	m.trace("entry() [EXIT]")
	m.retvoid()

	return fn, nil
}
