package mapper

import (
	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
)

// [codegen]
// void session_loop(sky_cursor *cursor, sky_map *result) {
// init:
//     int64_t prev_session_idle_time = cursor->session_idle_time;
//     cursor->session_idle_time = <SESSION_IDLE>;
//
// loop:
//     ...statements...
//     bool rc = sky_cursor_next_session(cursor)
//     if(rc) goto loop else goto exit;
//
// exit:
//     cursor->session_idle_time = prev_session_idle_time;
//     return;
// }
func (m *Mapper) codegenSessionLoop(node *ast.SessionLoop, tbl *ast.Symtable) (llvm.Value, error) {
	sig := llvm.FunctionType(m.context.VoidType(), []llvm.Type{llvm.PointerType(m.cursorType, 0), llvm.PointerType(m.hashmapType, 0)}, false)
	fn := llvm.AddFunction(m.module, "session_loop", sig)

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
	init := m.context.AddBasicBlock(fn, "init")
	loop := m.context.AddBasicBlock(fn, "loop")
	next_event := m.context.AddBasicBlock(fn, "next_event")
	exit := m.context.AddBasicBlock(fn, "exit")

	m.builder.SetInsertPointAtEnd(entry)
	m.trace(node.ClauseString())
	cursor_ref := m.alloca(llvm.PointerType(m.cursorType, 0), "cursor")
	result_ref := m.alloca(llvm.PointerType(m.hashmapType, 0), "result")
	m.store(fn.Param(0), cursor_ref)
	m.store(fn.Param(1), result_ref)
	m.br(init)

	// int64_t prev_session_idle_time = cursor->session_idle_time;
	// cursor->session_idle_time = <SESSION_IDLE>;
	m.builder.SetInsertPointAtEnd(init)
	prevSessionIdleTime := m.load(m.structgep(m.load(cursor_ref), cursorSessionIdleTimeElementIndex))
	m.store(m.constint(node.IdleDuration), m.structgep(m.load(cursor_ref), cursorSessionIdleTimeElementIndex))
	m.br(loop)

	// ...generate...
	// if(cursor->event->eof == 0) goto next_event else goto exit
	m.builder.SetInsertPointAtEnd(loop)
	for _, statementFn := range statementFns {
		m.call(statementFn, m.load(cursor_ref, ""), m.load(result_ref, ""))
	}
	m.condbr(m.icmp(llvm.IntEQ, m.load_eof(m.event_ref(cursor_ref)), m.constint(0)), next_event, exit)

	// rc = cursor_next_event(cursor);
	// if (rc == 0) goto loop_condition else goto exit;
	m.builder.SetInsertPointAtEnd(next_event)
	rc := m.call("cursor_next_event", m.load(cursor_ref))
	m.condbr(m.icmp(llvm.IntEQ, rc, m.constint(0)), loop, exit)

	// cursor->session_idle_time = prev_session_idle_time;
	// return;
	m.builder.SetInsertPointAtEnd(exit)
	m.store(prevSessionIdleTime, m.structgep(m.load(cursor_ref), cursorSessionIdleTimeElementIndex))
	m.retvoid()

	return fn, nil
}
