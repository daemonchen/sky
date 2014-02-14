package mapper

// #include <stdlib.h>
import "C"

import (
	"sync"
	"unsafe"

	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/hashmap"
	"github.com/szferi/gomdb"
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

	factorizer Factorizer

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

	entryFunc llvm.Value
}

// New creates a new Mapper instance.
func New(q *ast.Query, f Factorizer) (*Mapper, error) {
	mutex.Lock()
	defer mutex.Unlock()

	m := new(Mapper)
	m.TraceEnabled = true // FOR DEBUGGING ONLY
	m.factorizer = f

	m.context = llvm.NewContext()
	m.module = m.context.NewModule("mapper")
	m.builder = llvm.NewBuilder()

	var err error
	if err = q.Finalize(); err != nil {
		return nil, err
	}
	m.decls = q.VarDecls()

	if m.entryFunc, err = m.codegenQuery(q); err != nil {
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
func (m *Mapper) Map(lmdb_cursor *mdb.Cursor, prefix string, result *hashmap.Hashmap) error {
	cursor := sky_cursor_new(lmdb_cursor, prefix)
	defer sky_cursor_free(cursor)

	m.engine.RunFunction(m.entryFunc, []llvm.GenericValue{
		llvm.NewGenericValueFromPointer(unsafe.Pointer(cursor)),
		llvm.NewGenericValueFromPointer(unsafe.Pointer(result.C)),
	})
	return nil
}

// Iterate simply loops over every element of the raw cursor for benchmarking purposes.
func (m *Mapper) Iterate(c *mdb.Cursor) {
	sky_mdb_iterate(c)
}

// Dump writes the LLVM IR to STDERR.
func (m *Mapper) Dump() {
	m.module.Dump()
}
