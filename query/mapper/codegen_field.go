package mapper

import (
	"fmt"

	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query"
	"github.com/skydb/sky/query/ast"
)

// [codegen]
// void field(sky_cursor *cursor, sky_map *result) {
// fields:
//     ...generate...
//
// exit:
//     return;
// }
func (m *Mapper) codegenField(node *ast.Field, tbl *ast.Symtable, index int) (llvm.Value, error) {
	sig := llvm.FunctionType(m.context.VoidType(), []llvm.Type{llvm.PointerType(m.cursorType, 0), llvm.PointerType(m.hashmapType, 0)}, false)
	fn := llvm.AddFunction(m.module, node.Identifier()+"_field", sig)

	entry := m.context.AddBasicBlock(fn, "entry")
	body := m.context.AddBasicBlock(fn, "body")
	exit := m.context.AddBasicBlock(fn, "exit")

	m.builder.SetInsertPointAtEnd(entry)
	cursor := m.alloca(llvm.PointerType(m.cursorType, 0), "cursor")
	result := m.alloca(llvm.PointerType(m.hashmapType, 0), "result")
	m.store(fn.Param(0), cursor)
	m.store(fn.Param(1), result)
	m.br(body)

	m.builder.SetInsertPointAtEnd(body)
	event := m.load(m.structgep(m.load(cursor), cursorEventElementIndex), "event")
	if node.IsAggregate() {
		if err := m.codegenAggregateField(node, tbl, event, result); err != nil {
			return nilValue, err
		}
	} else {
		panic("UNIMPLEMENTED")
	}
	m.br(exit)

	m.builder.SetInsertPointAtEnd(exit)
	m.retvoid()

	return fn, nil
}

func (m *Mapper) codegenAggregateField(node *ast.Field, tbl *ast.Symtable, event llvm.Value, result llvm.Value) error {
	id := query.Hash(node.Identifier())

	// The "count" aggregation is a special case since it doesn't require an expression.
	if node.Aggregation == "count" {
		currentValue := m.call("sky_hashmap_get", m.load(result), m.constint(int(id)))
		newValue := m.builder.CreateAdd(currentValue, llvm.ConstInt(m.context.Int64Type(), 1, false), "")
		m.call("sky_hashmap_set", m.load(result), m.constint(int(id)), newValue)
		return nil
	}

	// Generate the field expression.
	expressionValue, err := m.codegenExpression(node.Expression, event, tbl)
	if err != nil {
		return err
	}

	// Retrieve hashmap value that matches expession type.
	fp := (expressionValue.Type().TypeKind() == llvm.DoubleTypeKind)
	var currentValue llvm.Value
	if fp {
		currentValue = m.call("sky_hashmap_get_double", m.load(result), m.constint(int(id)))
	} else {
		currentValue = m.call("sky_hashmap_get", m.load(result), m.constint(int(id)))
	}

	// Aggregate the value of the expression with the current value.
	var newValue llvm.Value
	switch node.Aggregation {
	case "sum":
		if fp {
			newValue = m.builder.CreateFAdd(currentValue, expressionValue, "")
		} else {
			newValue = m.builder.CreateAdd(currentValue, expressionValue, "")
		}
	default:
		return fmt.Errorf("Invalid aggregation type: %s", node.Aggregation)
	}

	// Update the hashmap value.
	if fp {
		m.call("sky_hashmap_set_double", m.load(result), m.constint(int(id)), newValue)
	} else {
		m.call("sky_hashmap_set", m.load(result), m.constint(int(id)), newValue)
	}

	return nil
}
