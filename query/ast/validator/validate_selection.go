package validator

import (
	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
)

func (v *validator) visitSelection(n *ast.Selection, tbl *ast.Symtable) {
	// Validate dimensions exist.
	for _, dimension := range n.Dimensions {
		if decl := tbl.Find(dimension.Name); decl == nil {
			v.err = errorf(n, "selection: dimension variable not found: %s", dimension.Name)
			return
		}
	}

	// Validate overlapping field names.
	identifiers := map[string]bool{}
	for _, field := range n.Fields {
		if identifiers[field.Identifier()] {
			v.err = errorf(n, "selection: field name already used: %s", field.Identifier())
			return
		}
		identifiers[field.Identifier()] = true
	}
}

func (v *validator) exitingSelection(n *ast.Selection, tbl *ast.Symtable) {
	// Validate dimensions data types.
	for _, dimension := range n.Dimensions {
		decl := tbl.Find(dimension.Name)
		switch decl.DataType {
		case db.String, db.Float:
			v.err = errorf(n, "selection: %s variables cannot be used as dimensions: %s", decl.DataType, dimension.Name)
			return
		}
	}
}
