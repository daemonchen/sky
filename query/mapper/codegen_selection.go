package mapper

import (
	"fmt"

	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/hashmap"
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
		result = m.call("sky_hashmap_submap", result, m.constint(int(hashmap.String(node.Name))))
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
		result = m.call("sky_hashmap_submap", result, m.constint(int(hashmap.String(dimension.Name))))
		result = m.call("sky_hashmap_submap", result, value)
	}
	m.br(fields_lbl)

	// ...generate fields...
	m.builder.SetInsertPointAtEnd(fields_lbl)
	for _, fieldFn := range fieldFns {
		m.call(fieldFn, m.load(cursor), result)
	}
	m.br(exit)

	m.builder.SetInsertPointAtEnd(exit)
	m.retvoid()

	return fn, nil
}
