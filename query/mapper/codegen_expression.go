package mapper

import (
	"errors"
	"fmt"

	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
)

// codegen generates LLVM code for a given AST expression.
func (m *Mapper) codegenExpression(node ast.Expression, event llvm.Value, symtable *ast.Symtable) (llvm.Value, error) {
	if node == nil {
		return nilValue, errors.New("mapper codegen expression: unexpected null node")
	}

	switch node := node.(type) {
	case *ast.BinaryExpression:
		return m.codegenBinaryExpression(node, event, symtable)
	case *ast.BooleanLiteral:
		return m.codegenBooleanLiteral(node, event, symtable)
	case *ast.IntegerLiteral:
		return m.codegenIntegerLiteral(node, event, symtable)
	case *ast.VarRef:
		return m.codegenVarRef(node, event, symtable)
	default:
		panic(fmt.Sprintf("mapper codegen expression: unexpected node type: %v", node))
	}
}
