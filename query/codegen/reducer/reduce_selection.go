package reducer

import (
	"fmt"
	"strconv"

	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/codegen/hashmap"
)

func (r *Reducer) reduceSelection(node *ast.Selection, h *hashmap.Hashmap, tbl *ast.Symtable) error {
	output := r.output

	// Drill into name.
	if node.Name != "" {
		h = h.Submap(hashmap.String(node.Name))
		output = submap(output, node.Name)
	}

	return r.reduceSelectionDimensions(node, h, output, node.Dimensions, tbl)
}

func (r *Reducer) reduceSelectionDimensions(node *ast.Selection, h *hashmap.Hashmap, output map[string]interface{}, dimensions []*ast.VarRef, tbl *ast.Symtable) error {
	// Reduce fields if we've reached the end of the dimensions.
	if len(dimensions) == 0 {
		for _, field := range node.Fields {
			if err := r.reduceField(field, h, output, tbl); err != nil {
				return err
			}
		}
		return nil
	}

	// Drill into first dimension
	dimension := dimensions[0]
	decl := tbl.Find(dimension.Name)
	if decl == nil {
		return fmt.Errorf("reduce: dimension not found: %s", dimension.Name)
	}

	// Drill into dimension name.
	h = h.Submap(hashmap.String(dimension.Name))
	output = submap(output, dimension.Name)

	// Iterate over dimension values.
	iterator := hashmap.NewIterator(h)
	for {
		key, _, ok := iterator.Next()
		if !ok {
			break
		}

		// Convert value to appropriate type based on variable decl.
		var keyString string
		switch decl.DataType {
		case db.String:
			return fmt.Errorf("reduce: string dimensions are not supported: %s", dimension.Name)
		case db.Float:
			return fmt.Errorf("reduce: float dimensions are not supported: %s", dimension.Name)
		case db.Integer:
			keyString = strconv.Itoa(int(key))
		case db.Factor:
			var err error
			decl := tbl.Find(dimension.Name)
			name := decl.Association
			if name == "" {
				name = decl.Name
			}
			if keyString, err = r.factorizer.Defactorize(name, uint64(key)); err != nil {
				return fmt.Errorf("reduce: factor not found: %s/%d", name, uint64(key))
			}
		case db.Boolean:
			if key == 0 {
				keyString = "false"
			} else {
				keyString = "true"
			}
		}

		// Drill into output map.
		m := submap(output, keyString)

		// Recursively drill into next dimension.
		if err := r.reduceSelectionDimensions(node, h.Submap(key), m, dimensions[1:], tbl); err != nil {
			return err
		}
	}

	return nil
}
