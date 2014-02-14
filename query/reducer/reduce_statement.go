package reducer

import (
	"fmt"

	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/hashmap"
)

// reduceStatement executes the reducer for a single statement.
func (r *Reducer) reduceStatement(node ast.Statement, h *hashmap.Hashmap, symtable *ast.Symtable) error {
	symtable = ast.NodeSymtable(node, symtable)

	switch node := node.(type) {
	case *ast.Condition:
		return r.reduceStatements(node.Statements, h, symtable)
	case *ast.EventLoop:
		return r.reduceStatements(node.Statements, h, symtable)
	case *ast.Selection:
		return r.reduceSelection(node, h, symtable)
	case *ast.SessionLoop:
		return r.reduceStatements(node.Statements, h, symtable)
	case *ast.VarDecl:
		if err := symtable.Add(node); err != nil {
			return err
		}
		return nil
	case *ast.Assignment:
		return nil
	default:
		panic(fmt.Sprintf("reduce: unexpected node type: %v", node))
	}
}

// reduceStatements executes the reducer for multiple statements.
func (r *Reducer) reduceStatements(nodes ast.Statements, h *hashmap.Hashmap, symtable *ast.Symtable) error {
	for _, node := range nodes {
		if err := r.reduceStatement(node, h, symtable); err != nil {
			return err
		}
	}
	return nil
}
