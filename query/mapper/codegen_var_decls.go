package mapper

import (
	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
)

// codegenVarDecls generates LLVM code for a list of variable declarations.
func (m *Mapper) codegenVarDecls(nodes ast.VarDecls, symtable *ast.Symtable) ([]llvm.Value, error) {
	values := []llvm.Value{}
	for _, node := range nodes {
		value, err := m.codegenStatement(node, symtable)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}
