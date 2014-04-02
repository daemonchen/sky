package mapper

/*
#include <stdlib.h>

typedef void entry_t(void*, void*);

void run_mapper(void *fn, void *cursor, void *result) {
	((entry_t*)fn)(cursor, result);
}

*/
import "C"

import (
	"sync"
	"unsafe"

	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query"
	"github.com/skydb/sky/query/ast"
)

var mutex sync.Mutex

func init() {
	llvm.LinkInJIT()
	llvm.InitializeNativeTarget()
}

// Mapper can compile a query and execute it against a cursor. The
// execution is single threaded and returns a nested map of data.
// The results can be combined using a Reducer.
type Mapper struct {
	TraceEnabled bool

	table *db.Table

	context llvm.Context
	module  llvm.Module
	engine  llvm.ExecutionEngine
	builder llvm.Builder

	decls ast.VarDecls

	cursorType    llvm.Type
	eventType     llvm.Type
	hashmapType   llvm.Type
	mdbCursorType llvm.Type
	mdbValType    llvm.Type

	entryFunc unsafe.Pointer
}

// New creates a new Mapper instance.
func New(q *ast.Query, t *db.Table) (*Mapper, error) {
	mutex.Lock()
	defer mutex.Unlock()

	m := new(Mapper)
	// m.TraceEnabled = true // FOR DEBUGGING ONLY
	m.table = t

	m.context = llvm.NewContext()
	m.module = m.context.NewModule("mapper")
	m.builder = llvm.NewBuilder()

	var err error
	if err = q.Finalize(); err != nil {
		return nil, err
	}
	m.decls = q.VarDecls()

	if _, err = m.codegenQuery(q); err != nil {
		return nil, err
	}
	if err = llvm.VerifyModule(m.module, llvm.ReturnStatusAction); err != nil {
		return nil, err
	}
	if m.engine, err = llvm.NewJITCompiler(m.module, 2); err != nil {
		return nil, err
	}

	// Optimization passes.
	pass := llvm.NewPassManager()
	defer pass.Dispose()

	pass.Add(m.engine.TargetData())
	pass.AddConstantPropagationPass()
	pass.AddInstructionCombiningPass()
	pass.AddPromoteMemoryToRegisterPass()
	pass.AddGVNPass()
	pass.AddCFGSimplificationPass()
	pass.Run(m.module)

	// Extract pointer to the function.
	m.entryFunc = m.engine.PointerToGlobal(m.engine.FindFunction("entry"))

	return m, nil
}

// Close cleans up resources after the mapper goes out of scope.
func (m *Mapper) Close() {
	mutex.Lock()
	defer mutex.Unlock()

	m.builder.Dispose()
	m.engine.Dispose()
}

// Execute runs the entry function on the execution engine.
func (m *Mapper) Map(c *db.Cursor, prefix string, result *query.Hashmap) error {
	cursor := sky_cursor_new(c.Cursor, prefix)
	defer sky_cursor_free(cursor)

	C.run_mapper(m.entryFunc, unsafe.Pointer(cursor), unsafe.Pointer(result.C))

	return nil
}

// Iterate simply loops over every element of the raw cursor for benchmarking purposes.
func (m *Mapper) Iterate(c *db.Cursor) {
	sky_mdb_iterate(c.Cursor)
}

// Dump writes the LLVM IR to STDERR.
func (m *Mapper) Dump() {
	m.module.Dump()
}

// Copy creates a clone of this mapper.
func (m *Mapper) Copy() {
	m.module.Dump()
}
