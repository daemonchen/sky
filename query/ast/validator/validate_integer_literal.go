package validator

import (
	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
)

func (v *validator) visitIntegerLiteral(n *ast.IntegerLiteral, tbl *ast.Symtable) {
	v.dataTypes[n] = db.IntegerDataType
}
