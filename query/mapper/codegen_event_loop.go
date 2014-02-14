package mapper

import (
	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
)

// [codegen]
// void event_loop(sky_cursor *cursor, sky_map *result) {
// loop:
//     ...statements...
//     bool rc = cursor_next_event(cursor)
//     if(rc) goto loop else goto exit;
//
// exit:
//     return;
// }
func (m *Mapper) codegenEventLoop(node *ast.EventLoop, tbl *ast.Symtable) (llvm.Value, error) {
	sig := llvm.FunctionType(m.context.VoidType(), []llvm.Type{llvm.PointerType(m.cursorType, 0), llvm.PointerType(m.hashmapType, 0)}, false)
	fn := llvm.AddFunction(m.module, "event_loop", sig)

	// Generate functions for child statements.
	var statementFns []llvm.Value
	for _, statement := range node.Statements {
		statementFn, err := m.codegenStatement(statement, tbl)
		if err != nil {
			return nilValue, err
		}
		statementFns = append(statementFns, statementFn)
	}

	entry := m.context.AddBasicBlock(fn, "entry")
	loop := m.context.AddBasicBlock(fn, "loop")
	next_event := m.context.AddBasicBlock(fn, "event")
	exit := m.context.AddBasicBlock(fn, "exit")

	m.builder.SetInsertPointAtEnd(entry)
	m.trace("FOR EACH EVENT")
	cursor_ref := m.alloca(llvm.PointerType(m.cursorType, 0), "cursor")
	result_ref := m.alloca(llvm.PointerType(m.hashmapType, 0), "result")
	m.store(fn.Param(0), cursor_ref)
	m.store(fn.Param(1), result_ref)
	m.builder.CreateBr(loop)

	// ...generate...
	// if(cursor->event->eos == 0) goto next_event else goto exit;
	m.builder.SetInsertPointAtEnd(loop)
	for _, statementFn := range statementFns {
		m.call(statementFn, m.load(cursor_ref, ""), m.load(result_ref, ""))
	}
	m.condbr(m.icmp(llvm.IntEQ, m.load_eos(m.event_ref(cursor_ref), "eos"), m.constint(0)), next_event, exit)

	// rc = cursor_next_event(cursor);
	// if(rc == 0) goto loop else goto exit;
	m.builder.SetInsertPointAtEnd(next_event)
	rc := m.call("cursor_next_event", m.load(cursor_ref))
	m.condbr(m.icmp(llvm.IntEQ, rc, m.constint(0)), loop, exit)

	// return;
	m.builder.SetInsertPointAtEnd(exit)
	m.retvoid()

	return fn, nil
}
