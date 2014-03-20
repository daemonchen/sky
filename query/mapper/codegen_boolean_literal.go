package mapper

import (
	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
)

func (m *Mapper) codegenBooleanLiteral(node *ast.BooleanLiteral, event llvm.Value, tbl *ast.Symtable) (llvm.Value, error) {
	if node.Value {
		return llvm.ConstInt(m.context.Int64Type(), 1, false), nil
	}
	return llvm.ConstInt(m.context.Int64Type(), 0, false), nil
}
