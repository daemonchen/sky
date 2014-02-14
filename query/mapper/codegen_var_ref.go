package mapper

import (
	"fmt"

	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
)

func (m *Mapper) codegenVarRef(node *ast.VarRef, event llvm.Value, tbl *ast.Symtable) (llvm.Value, error) {
	m.trace(node.String())

	decl := tbl.Find(node.Name)
	if decl == nil {
		return nilValue, fmt.Errorf("Unknown variable reference: %s", node.Name)
	}
	return m.load(m.structgep(event, decl.Index()), node.Name), nil
}
