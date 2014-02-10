package validator

import (
	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
)

func (v *validator) visitField(n *ast.Field, tbl *ast.Symtable) {
	// Validate non-aggregated fields (currently unsupported).
	if n.Aggregation == "" {
		v.err = errorf(n, "field: non-aggregated fields not currently supported")
		return
	}

	// Validate aggregation method.
	switch n.Aggregation {
	case "count", "sum":
	default:
		v.err = errorf(n, "field: unsupported aggregation type: %s", n.Aggregation)
		return
	}

	// Validate distinct (temporarily unsupported).
	if n.Distinct {
		v.err = errorf(n, "field: DISTINCT is currently unsupported")
		return
	}

	// Disallow expressions with count().
	if n.Aggregation == "count" && n.Expression != nil {
		v.err = errorf(n, "field: count() does not accept an expression")
		return
	}
}

func (v *validator) exitingField(n *ast.Field, tbl *ast.Symtable) {
	if n.Expression != nil {
		expressionType := v.dataTypes[n.Expression]

		switch expressionType {
		case db.IntegerDataType, db.FloatDataType:
		default:
			v.err = errorf(n, "field: invalid expression data type: %s", expressionType)
		}
	}
}
