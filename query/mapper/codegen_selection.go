package mapper

import (
	"fmt"

	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query"
	"github.com/skydb/sky/query/ast"
)

// [codegen]
// void selection(sky_cursor *cursor, sky_map *result) {
// exit:
//     return;
// }
func (m *Mapper) codegenSelection(node *ast.Selection, tbl *ast.Symtable) (llvm.Value, error) {
	sig := llvm.FunctionType(m.context.VoidType(), []llvm.Type{llvm.PointerType(m.cursorType, 0), llvm.PointerType(m.hashmapType, 0)}, false)
	fn := llvm.AddFunction(m.module, "selection", sig)

	// Generate functions for fields.
	var fieldFns []llvm.Value
	for index, field := range node.Fields {
		fieldFn, err := m.codegenField(field, tbl, index)
		if err != nil {
			return nilValue, err
		}
		fieldFns = append(fieldFns, fieldFn)
	}

	entry := m.context.AddBasicBlock(fn, "entry")
	name_lbl := m.context.AddBasicBlock(fn, "name")
	dimensions_lbl := m.context.AddBasicBlock(fn, "dimensions")
	nonaggregate_submap_lbl := m.context.AddBasicBlock(fn, "nonaggregate_submap")
	fields_lbl := m.context.AddBasicBlock(fn, "fields")
	exit := m.context.AddBasicBlock(fn, "exit")

	m.builder.SetInsertPointAtEnd(entry)
	m.trace(node.String())
	cursor := m.alloca(llvm.PointerType(m.cursorType, 0), "cursor")
	result_ref := m.alloca(llvm.PointerType(m.hashmapType, 0), "result")
	m.store(fn.Param(0), cursor)
	m.store(fn.Param(1), result_ref)
	event := m.load(m.structgep(m.load(cursor), cursorEventElementIndex), "event")
	result := m.load(result_ref)
	m.br(name_lbl)

	m.builder.SetInsertPointAtEnd(name_lbl)
	if node.Name != "" {
		result = m.call("sky_hashmap_submap", result, m.constint(int(query.Hash(node.Name))))
	}
	m.br(dimensions_lbl)

	// Traverse to the appropriate hashmap in the results.
	m.builder.SetInsertPointAtEnd(dimensions_lbl)
	for _, dimension := range node.Dimensions {
		decl := tbl.Find(dimension.Name)
		if decl == nil {
			return nilValue, fmt.Errorf("Dimension variable not found: %s", dimension.Name)
		}
		value := m.load(m.structgep(event, decl.Index()))
		result = m.call("sky_hashmap_submap", result, m.constint(int(query.Hash(dimension.Name))))
		result = m.call("sky_hashmap_submap", result, value)
	}
	m.br(nonaggregate_submap_lbl)

	// Non-aggregate queries are hashmaps treated like arrays of hashmaps.
	// The index 0 value is the size of the hashmap. Value objectss are keys 1..*.
	m.builder.SetInsertPointAtEnd(nonaggregate_submap_lbl)
	hashmap := result
	if node.HasNonAggregateFields() {
		// Increment current count.
		countValue := m.call("sky_hashmap_get", result, m.constint(int(0)))
		countValue = m.add(countValue, m.constint(1))
		m.call("sky_hashmap_set", result, m.constint(0), countValue)

		// Create value object hashmap.
		hashmap = m.call("sky_hashmap_submap", result, countValue)
	}
	m.br(fields_lbl)

	// ...generate fields...
	m.builder.SetInsertPointAtEnd(fields_lbl)
	for _, fieldFn := range fieldFns {
		m.call(fieldFn, m.load(cursor), hashmap)
	}
	m.br(exit)

	m.builder.SetInsertPointAtEnd(exit)
	m.retvoid()

	return fn, nil
}
