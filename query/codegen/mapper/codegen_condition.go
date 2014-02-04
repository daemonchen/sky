package mapper

import (
	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
)

// [codegen]
// void condition(sky_cursor *cursor, sky_map *result) {
//     int index = 0;
//
// loop_range_condition:
//     if(index >= within_range_start) goto loop_expr_condition else goto loop_end;
//
// loop_expr_condition:
//     if(expressionValue == true) goto loop_body else goto loop_end;
//
// loop_body:
//     ...statements...
//     goto loop_end;
//
// loop_end:
//     if(index < within_range_end) goto loop_iterate else goto exit;
//
// loop_iterate:
//     index = index + 1;
//     bool rc = cursor_next_event(cursor);
//     if(rc) goto loop_range_condition else goto exit;
//
// exit:
//     return;
// }
func (m *Mapper) codegenCondition(node *ast.Condition, tbl *ast.Symtable) (llvm.Value, error) {
	sig := llvm.FunctionType(m.context.VoidType(), []llvm.Type{llvm.PointerType(m.cursorType, 0), llvm.PointerType(m.hashmapType, 0)}, false)
	fn := llvm.AddFunction(m.module, "condition", sig)

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
	loop_range_condition := m.context.AddBasicBlock(fn, "loop_range_condition")
	loop_expr_condition := m.context.AddBasicBlock(fn, "loop_expr_condition")
	loop_body := m.context.AddBasicBlock(fn, "loop_body")
	loop_end := m.context.AddBasicBlock(fn, "loop_end")
	loop_iterate := m.context.AddBasicBlock(fn, "loop_iterate")
	exit := m.context.AddBasicBlock(fn, "exit")

	m.builder.SetInsertPointAtEnd(entry)
	m.trace(node.ClauseString())
	cursor := m.alloca(llvm.PointerType(m.cursorType, 0), "cursor")
	result := m.alloca(llvm.PointerType(m.hashmapType, 0), "result")
	index := m.alloca(m.context.Int64Type(), "index")
	m.store(fn.Param(0), cursor)
	m.store(fn.Param(1), result)
	m.store(m.constint(0), index)
	m.br(loop_range_condition)

	m.builder.SetInsertPointAtEnd(loop_range_condition)
	m.condbr(m.icmp(llvm.IntUGE, m.load(index), m.constint(node.Start)), loop_expr_condition, loop_end)

	m.builder.SetInsertPointAtEnd(loop_expr_condition)
	event := m.load(m.structgep(m.load(cursor), cursorEventElementIndex), "event")
	exprValue, err := m.codegenExpression(node.Expression, event, tbl)
	if err != nil {
		return nilValue, err
	}
	m.condbr(exprValue, loop_body, loop_end)

	m.builder.SetInsertPointAtEnd(loop_body)
	for _, statementFn := range statementFns {
		m.call(statementFn, m.load(cursor), m.load(result))
	}
	m.br(loop_end)

	m.builder.SetInsertPointAtEnd(loop_end)
	m.condbr(m.and(
		m.icmp(llvm.IntULT, m.load(index), m.constint(node.End)),
		m.icmp(llvm.IntEQ, m.load_eos(m.event_ref(cursor)), m.constint(0))),
		loop_iterate, exit)

	// index++;
	// rc = cursor_next_event(cursor);
	// if(rc == 0) goto loop_range_condition else goto exit;
	m.builder.SetInsertPointAtEnd(loop_iterate)
	m.store(m.add(m.load(index), m.constint(1)), index)
	rc := m.call("cursor_next_event", m.load(cursor))
	m.condbr(m.icmp(llvm.IntEQ, rc, m.constint(0)), loop_range_condition, exit)

	m.builder.SetInsertPointAtEnd(exit)
	m.retvoid()

	return fn, nil
}
