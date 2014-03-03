package reducer

import (
	"fmt"

	"github.com/skydb/sky/query"
	"github.com/skydb/sky/query/ast"
)

func (r *Reducer) reduceField(node *ast.Field, h *query.Hashmap, output map[string]interface{}, tbl *ast.Symtable) error {
	// Ignore fields that have the same path as a previous field.
	if node.Reducible() {
		return nil
	}

	identifier := node.Identifier()
	valueType := h.ValueType(query.Hash(node.Identifier()))

	switch valueType {
	case query.IntValueType:
		prevValue, _ := output[identifier].(int)
		switch node.Aggregation {
		case "count", "sum":
			output[identifier] = prevValue + int(h.Get(query.Hash(node.Identifier())))
		default:
			return fmt.Errorf("reduce: unsupported int aggregation type: %s", node.Aggregation)
		}

	case query.DoubleValueType:
		prevValue, _ := output[identifier].(float64)
		switch node.Aggregation {
		case "count", "sum":
			output[identifier] = prevValue + h.GetDouble(query.Hash(node.Identifier()))
		default:
			return fmt.Errorf("reduce: unsupported int aggregation type: %s", node.Aggregation)
		}
	}

	return nil
}
