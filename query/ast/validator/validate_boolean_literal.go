package validator

import (
	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
)

func (v *validator) visitBooleanLiteral(n *ast.BooleanLiteral, tbl *ast.Symtable) {
	v.dataTypes[n] = db.BooleanDataType
}
