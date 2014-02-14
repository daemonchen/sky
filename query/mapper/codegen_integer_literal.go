package mapper

import (
	"fmt"

	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
)

func (m *Mapper) codegenIntegerLiteral(node *ast.IntegerLiteral, event llvm.Value, tbl *ast.Symtable) (llvm.Value, error) {
	return llvm.ConstIntFromString(m.context.Int64Type(), fmt.Sprintf("%d", node.Value), 10), nil
}
