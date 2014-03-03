package reducer

import (
	"github.com/skydb/sky/query"
	"github.com/skydb/sky/query/ast"
)

func (r *Reducer) reduceQuery(q *ast.Query, h *query.Hashmap) error {
	tbl := ast.NewSymtable(nil)
	if err := tbl.Add(q.VarDecls()...); err != nil {
		return err
	}

	if err := r.reduceStatements(q.Statements, h, tbl); err != nil {
		return err
	}

	return nil
}
