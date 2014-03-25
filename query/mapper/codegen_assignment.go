package mapper

import (
	"fmt"

	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
)

// [codegen]
// void assignment(sky_cursor *cursor, sky_map *result) {
//     event->field = expr;
// }
func (m *Mapper) codegenAssignment(node *ast.Assignment, tbl *ast.Symtable) (llvm.Value, error) {
	sig := llvm.FunctionType(m.context.VoidType(), []llvm.Type{llvm.PointerType(m.cursorType, 0), llvm.PointerType(m.hashmapType, 0)}, false)
	fn := llvm.AddFunction(m.module, "assignment", sig)

	entry := m.context.AddBasicBlock(fn, "entry")
	assign := m.context.AddBasicBlock(fn, "assign")
	exit := m.context.AddBasicBlock(fn, "exit")

	m.builder.SetInsertPointAtEnd(entry)
	m.trace(node.String())
	cursor := m.alloca(llvm.PointerType(m.cursorType, 0), "cursor")
	result := m.alloca(llvm.PointerType(m.hashmapType, 0), "result")
	m.store(fn.Param(0), cursor)
	m.store(fn.Param(1), result)
	m.br(assign)

	m.builder.SetInsertPointAtEnd(assign)

	// Calculate LHS variable ptr.
	event := m.load(m.structgep(m.load(cursor), cursorEventElementIndex), "event")
	decl := tbl.Find(node.Target.Name)
	if decl == nil {
		return nilValue, fmt.Errorf("Unknown variable in assignment: %s", node.Target.Name)
	}

	// Codegen RHS expression.
	var expressionValue llvm.Value
	switch expr := node.Expression.(type) {
	case *ast.StringLiteral:
		decl := tbl.Find(node.Target.Name)
		name := decl.Association
		if name == "" {
			name = decl.Name
		}

		// Find property on table.
		p, err := m.table.Property(name)
		if err != nil {
			return nilValue, err
		} else if p == nil {
			return nilValue, &Error{"variable not found: " + name, nil}
		}

		// Factorize value.
		id, err := p.Factorize(expr.Value)
		if err != nil {
			return nilValue, err
		}
		expressionValue = m.constint(int(id))

	default:
		var err error
		expressionValue, err = m.codegenExpression(expr, event, tbl)
		if err != nil {
			return nilValue, err
		}
	}

	// Store expression value in variable.
	m.store(expressionValue, m.structgep(event, decl.Index()))
	m.br(exit)

	m.builder.SetInsertPointAtEnd(exit)
	m.retvoid()

	return fn, nil
}
