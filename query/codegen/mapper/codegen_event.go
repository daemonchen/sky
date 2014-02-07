package mapper

import (
	"fmt"

	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/core"
	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
)

const (
	eventEofElementIndex       = 0
	eventEosElementIndex       = 1
	eventTimestampElementIndex = 2
)

// [codegen]
// typedef struct {
//     ...
//     struct  { void *data; int64_t sz; } string_var;
//     int64_t factor_var;
//     int64_t integer_var;
//     double  float_var;
//     int64_t boolean_var;
//     ...
// } sky_event;
func (m *Mapper) codegenEventType() llvm.Type {
	// Append variable declarations.
	fields := []llvm.Type{}
	for _, decl := range m.decls {
		var field llvm.Type

		switch decl.DataType {
		case core.StringDataType:
			field = m.context.StructType([]llvm.Type{
				m.context.Int64Type(),
				llvm.PointerType(m.context.VoidType(), 0),
			}, false)
		case core.IntegerDataType, core.FactorDataType:
			field = m.context.Int64Type()
		case core.FloatDataType:
			field = m.context.DoubleType()
		case core.BooleanDataType:
			field = m.context.Int64Type()
		}

		fields = append(fields, field)
	}

	// Return composite type.
	typ := m.context.StructCreateNamed("sky_event")
	typ.StructSetBody(fields, false)
	return typ
}

// [codegen]
// int64_t sky_cursor_next_event(sky_cursor *)
// int64_t cursor_next_event(sky_cursor *) {
//     MDB_val key, data;
//     goto init;
//
// init:
//     copy_event(cursor->event, cursor->next_event);
//     copy_permanent_variables(cursor->event, cursor->next_event);
//     clear_transient_variables(cursor->next_event);
//     goto mdb_cursor_get;
//
// mdb_cursor_get:
//     bool rc = mdb_cursor_get(cursor->lmdb_cursor, &key, &data, MDB_NEXT_DUP);
//     if(rc == 0) goto read_event else goto error;
//
// read_event:
//     rc = read_event(cursor->next_event);
//     if(rc) goto copy_event else goto error;
//
// error:
//     cursor->next_event->eof = 1;
//     cursor->next_event->eos = 1;
//     return -1;
//
// exit:
//     return 0;
// }
func (m *Mapper) codegenCursorNextEventFunc() {
	fntype := llvm.FunctionType(m.context.Int64Type(), []llvm.Type{llvm.PointerType(m.cursorType, 0)}, false)
	fn := llvm.AddFunction(m.module, "cursor_next_event", fntype)

	m.codegenEventCopyFunc("sky_event_copy", nil)
	m.codegenEventCopyFunc("sky_event_copy_declared", func(decl *ast.VarDecl) bool {
		return decl.IsDeclared()
	})
	m.codegenEventCopyFunc("sky_event_copy_permanent", func(decl *ast.VarDecl) bool {
		return decl.IsPermanent()
	})
	m.codegenEventResetFunc("sky_event_reset", nil)
	m.codegenEventResetFunc("sky_event_reset_transient", func(decl *ast.VarDecl) bool {
		return decl.IsTransient()
	})
	m.codegenReadEventFunc()

	entry := m.context.AddBasicBlock(fn, "entry")
	swap := m.context.AddBasicBlock(fn, "swap")
	init := m.context.AddBasicBlock(fn, "init")
	mdb_cursor_get := m.context.AddBasicBlock(fn, "mdb_cursor_get")
	read_event := m.context.AddBasicBlock(fn, "read_event")
	check_eos := m.context.AddBasicBlock(fn, "check_eos")
	set_eos := m.context.AddBasicBlock(fn, "set_eos")
	set_eof := m.context.AddBasicBlock(fn, "set_eof")
	error_lbl := m.context.AddBasicBlock(fn, "error")
	exit := m.context.AddBasicBlock(fn, "exit")

	// Allocate stack.
	m.builder.SetInsertPointAtEnd(entry)
	m.trace("cursor_next_event()")
	cursor := m.alloca(llvm.PointerType(m.cursorType, 0), "cursor")
	key := m.alloca(m.mdbValType, "key")
	data := m.alloca(m.mdbValType, "data")
	m.store(fn.Param(0), cursor)
	m.br(swap)

	// sky_event_copy_declared(cursor->next_event, cursor->event);
	// tmp = cursor->event;
	// cursor->event = cursor->next_event;
	// cursor->next_event = tmp;
	// cursor->next_event->eos = 0;
	// goto init;
	m.builder.SetInsertPointAtEnd(swap)
	m.call("sky_event_copy_declared", m.load(m.next_event_ref(cursor), "next_event"), m.load(m.event_ref(cursor), "event"))
	tmp := m.load(m.event_ref(cursor), "event")
	m.store(m.load(m.next_event_ref(cursor), "next_event"), m.event_ref(cursor))
	m.store(tmp, m.next_event_ref(cursor))
	m.store(m.constint(0), m.structgep(m.load(m.next_event_ref(cursor)), eventEosElementIndex))
	m.br(init)

	// sky_event_copy_permanent(next_event, event);
	// sky_event_reset_transient(next_event);
	// goto mdb_cursor_get;
	m.builder.SetInsertPointAtEnd(init)
	event := m.load(m.event_ref(cursor), "event")
	next_event := m.load(m.next_event_ref(cursor), "next_event")
	m.call("sky_event_copy_permanent", next_event, event)
	m.call("sky_event_reset_transient", next_event)
	m.br(mdb_cursor_get)

	// rc = mdb_cursor_get(cursor->lmdb_cursor, &key, &data, MDB_NEXT_DUP);
	// if(rc == 0) goto read_event else goto set_eof;
	m.builder.SetInsertPointAtEnd(mdb_cursor_get)
	lmdb_cursor := m.load(m.structgep(m.load(cursor, ""), cursorLMDBCursorElementIndex, ""), "lmdb_cursor")
	rc := m.builder.CreateCall(m.module.NamedFunction("mdb_cursor_get"), []llvm.Value{lmdb_cursor, key, data, llvm.ConstInt(m.context.Int64Type(), MDB_NEXT_DUP, false)}, "rc")
	m.condbr(m.builder.CreateICmp(llvm.IntEQ, rc, llvm.ConstInt(m.context.Int64Type(), 0, false), ""), read_event, set_eof)

	// rc = cursor_read_event(next_event, ptr);
	// if(rc == 0) goto check_eos else goto error;
	m.builder.SetInsertPointAtEnd(read_event)
	ptr := m.load(m.structgep(data, 1, ""), "ptr")
	rc = m.call("cursor_read_event", next_event, ptr)
	m.condbr(m.icmp(llvm.IntEQ, rc, m.constint(0)), check_eos, error_lbl)

	// timestamp = event->timestamp;
	// next_timestamp = next_event->timestamp;
	// session_idle_time = cursor->session_idle_time;
	// max_timestamp = timestamp + session_idle_time;
	// if(max_timestamp < next_timestamp) goto set_eos else goto exit;
	m.builder.SetInsertPointAtEnd(check_eos)
	timestamp := m.load(m.structgep(event, eventTimestampElementIndex))
	nextTimestamp := m.load(m.structgep(next_event, eventTimestampElementIndex))
	sessionIdleTime := m.load(m.structgep(m.load(cursor), cursorSessionIdleTimeElementIndex))
	maxTimestamp := m.add(timestamp, sessionIdleTime, "max_timestamp")
	m.condbr(m.and(m.icmp(llvm.IntSGT, sessionIdleTime, m.constint(0)), m.icmp(llvm.IntSLE, maxTimestamp, nextTimestamp)), set_eos, exit)

	// event->eos = 1;
	m.builder.SetInsertPointAtEnd(set_eos)
	m.store(m.constint(1), m.structgep(event, eventEosElementIndex))
	m.br(exit)

	// event->eof = 1;
	// event->eos = 1;
	// next_event->eof = 1;
	// next_event->eos = 1;
	// goto exit;
	m.builder.SetInsertPointAtEnd(set_eof)
	m.store(m.constint(1), m.structgep(event, eventEofElementIndex))
	m.store(m.constint(1), m.structgep(event, eventEosElementIndex))
	m.store(m.constint(1), m.structgep(next_event, eventEofElementIndex))
	m.store(m.constint(1), m.structgep(next_event, eventEosElementIndex))
	m.br(exit)

	// event->eof = 1;
	// event->eos = 1;
	// next_event->eof = 1;
	// next_event->eos = 1;
	// return -1;
	m.builder.SetInsertPointAtEnd(error_lbl)
	m.store(m.constint(1), m.structgep(event, eventEofElementIndex))
	m.store(m.constint(1), m.structgep(event, eventEosElementIndex))
	m.store(m.constint(1), m.structgep(next_event, eventEofElementIndex))
	m.store(m.constint(1), m.structgep(next_event, eventEosElementIndex))
	m.ret(m.constint(-1))

	// return 0;
	m.builder.SetInsertPointAtEnd(exit)
	m.ret(m.constint(0))
}

// [codegen]
// void sky_event_copy(sky_event *dest, sky_event *src) {
//     ...
//     dest->field = src->field;
//     ...
// }
func (m *Mapper) codegenEventCopyFunc(functionName string, filter func(*ast.VarDecl) bool) llvm.Value {
	fntype := llvm.FunctionType(m.context.VoidType(), []llvm.Type{llvm.PointerType(m.eventType, 0), llvm.PointerType(m.eventType, 0)}, false)
	fn := llvm.AddFunction(m.module, functionName, fntype)

	m.builder.SetInsertPointAtEnd(m.context.AddBasicBlock(fn, "entry"))
	m.trace(functionName + "()")
	dest := m.alloca(llvm.PointerType(m.eventType, 0), "dest")
	src := m.alloca(llvm.PointerType(m.eventType, 0), "src")
	m.store(fn.Param(0), dest)
	m.store(fn.Param(1), src)

	for _, decl := range m.decls {
		if filter == nil || filter(decl) {
			switch decl.DataType {
			case core.IntegerDataType, core.FactorDataType, core.FloatDataType, core.BooleanDataType:
				m.store(m.load(m.structgep(m.load(src, ""), decl.Index())), m.structgep(m.load(dest, ""), decl.Index()))
			}
		}
	}

	m.retvoid()
	return fn
}

// [codegen]
// void sky_event_reset(sky_event *event) {
//     ...
//     event->field = 0;
//     ...
// }
func (m *Mapper) codegenEventResetFunc(functionName string, filter func(*ast.VarDecl) bool) llvm.Value {
	fntype := llvm.FunctionType(m.context.VoidType(), []llvm.Type{llvm.PointerType(m.eventType, 0)}, false)
	fn := llvm.AddFunction(m.module, functionName, fntype)

	m.builder.SetInsertPointAtEnd(m.context.AddBasicBlock(fn, "entry"))
	m.trace(functionName + "()")
	event := m.alloca(llvm.PointerType(m.eventType, 0), "event")
	m.store(fn.Param(0), event)

	for index, decl := range m.decls {
		if filter == nil || filter(decl) {
			switch decl.DataType {
			case core.StringDataType:
				panic("NOT YET IMPLEMENTED: clear_event [string]")
			case core.IntegerDataType, core.FactorDataType, core.BooleanDataType:
				m.store(m.constint(0), m.structgep(m.load(event), index, decl.Name))
			case core.FloatDataType:
				m.store(m.constfloat(0), m.structgep(m.load(event), index, decl.Name))
			}
		}
	}

	m.retvoid()
	return fn
}

// [codegen]
// int64_t read_event(sky_event *event, void *ptr)
func (m *Mapper) codegenReadEventFunc() llvm.Value {
	fntype := llvm.FunctionType(m.context.Int64Type(), []llvm.Type{llvm.PointerType(m.eventType, 0), llvm.PointerType(m.context.Int8Type(), 0)}, false)
	fn := llvm.AddFunction(m.module, "cursor_read_event", fntype)
	fn.SetFunctionCallConv(llvm.CCallConv)

	read_decls := ast.VarDecls{}
	read_labels := []llvm.BasicBlock{}

	entry := m.context.AddBasicBlock(fn, "entry")
	read_ts := m.context.AddBasicBlock(fn, "read_ts")
	read_map := m.context.AddBasicBlock(fn, "read_map")
	loop := m.context.AddBasicBlock(fn, "loop")
	loop_read_key := m.context.AddBasicBlock(fn, "loop_read_key")
	loop_read_value := m.context.AddBasicBlock(fn, "loop_read_value")
	loop_skip := m.context.AddBasicBlock(fn, "loop_skip")
	for _, decl := range m.decls {
		if decl.Id != 0 {
			read_decls = append(read_decls, decl)
			read_labels = append(read_labels, m.context.AddBasicBlock(fn, decl.Name))
		}
	}
	error_lbl := m.context.AddBasicBlock(fn, "error")
	exit := m.context.AddBasicBlock(fn, "exit")

	// entry:
	//     int64_t sz;
	//     int64_t variable_id;
	//     int64_t key_count;
	//     int64_t key_index = 0;
	m.builder.SetInsertPointAtEnd(entry)
	m.trace("cursor_read_event()")
	event := m.alloca(llvm.PointerType(m.eventType, 0), "event")
	ptr := m.alloca(llvm.PointerType(m.context.Int8Type(), 0), "ptr")
	sz := m.alloca(m.context.Int64Type(), "sz")
	variable_id := m.alloca(m.context.Int64Type(), "variable_id")
	key_count := m.alloca(m.context.Int64Type(), "key_count")
	key_index := m.alloca(m.context.Int64Type(), "key_index")
	m.store(fn.Param(0), event)
	m.store(fn.Param(1), ptr)
	m.store(llvm.ConstInt(m.context.Int64Type(), 0, false), key_index)
	m.br(read_ts)

	// read_ts:
	//     int64_t ts = *((int64_t*)ptr);
	//     event->timestamp = ts;
	//     ptr += 8;
	m.builder.SetInsertPointAtEnd(read_ts)
	ts_value := m.load(m.builder.CreateBitCast(m.load(ptr, ""), llvm.PointerType(m.context.Int64Type(), 0), ""), "ts_value")
	native_ts_value := m.call("llvm.bswap.i64", ts_value)
	timestamp_value := m.builder.CreateLShr(native_ts_value, llvm.ConstInt(m.context.Int64Type(), db.SecondsBitOffset, false), "timestamp_value")
	event_timestamp := m.structgep(m.load(event, ""), eventTimestampElementIndex, "event_timestamp")
	m.store(timestamp_value, event_timestamp)
	m.store(m.builder.CreateGEP(m.load(ptr, ""), []llvm.Value{llvm.ConstInt(m.context.Int64Type(), 8, false)}, ""), ptr)
	m.br(read_map)

	// read_map:
	//     key_index = 0;
	//     key_count = minipack_unpack_map(ptr, &sz);
	//     ptr += sz;
	//     if(sz != 0) goto loop else goto error
	m.builder.SetInsertPointAtEnd(read_map)
	m.store(m.call("minipack_unpack_map", m.load(ptr, ""), sz), key_count)
	m.store(m.builder.CreateGEP(m.load(ptr, ""), []llvm.Value{m.load(sz, "")}, ""), ptr)
	m.condbr(m.builder.CreateICmp(llvm.IntNE, m.load(sz, ""), llvm.ConstInt(m.context.Int64Type(), 0, false), ""), loop, error_lbl)

	// loop:
	//     if(key_index < key_count) goto loop_read_key else goto exit;
	m.builder.SetInsertPointAtEnd(loop)
	m.condbr(m.builder.CreateICmp(llvm.IntSLT, m.load(key_index, ""), m.load(key_count, ""), ""), loop_read_key, exit)

	// loop_read_key:
	//     variable_id = minipack_unpack_int(ptr, sz)
	//     ptr += sz;
	//     if(sz != 0) goto loop_read_value else goto error;
	m.builder.SetInsertPointAtEnd(loop_read_key)
	m.store(m.builder.CreateCall(m.module.NamedFunction("minipack_unpack_int"), []llvm.Value{m.load(ptr, ""), sz}, ""), variable_id)
	m.store(m.builder.CreateAdd(m.load(key_index), llvm.ConstInt(m.context.Int64Type(), 1, false), ""), key_index)
	m.store(m.builder.CreateGEP(m.load(ptr, ""), []llvm.Value{m.load(sz, "")}, ""), ptr)
	m.condbr(m.builder.CreateICmp(llvm.IntNE, m.load(sz, ""), llvm.ConstInt(m.context.Int64Type(), 0, false), ""), loop_read_value, error_lbl)

	// loop_read_value:
	//     index += 1;
	//     switch(variable_id) {
	//     ...
	//     default:
	//         goto loop_skip;
	//     }
	m.builder.SetInsertPointAtEnd(loop_read_value)
	sw := m.builder.CreateSwitch(m.load(variable_id, ""), loop_skip, len(read_decls))
	for i, decl := range read_decls {
		sw.AddCase(llvm.ConstIntFromString(m.context.Int64Type(), fmt.Sprintf("%d", decl.Id), 10), read_labels[i])
	}

	// XXX:
	//     event->XXX = minipack_unpack_XXX(ptr, sz);
	//     ptr += sz;
	//     if(sz != 0) goto loop else goto loop_skip;
	for i, decl := range read_decls {
		m.builder.SetInsertPointAtEnd(read_labels[i])

		if decl.DataType == core.StringDataType {
			panic("NOT YET IMPLEMENTED: read_event [string]")
		}

		var minipack_func_name string
		switch decl.DataType {
		case core.IntegerDataType, core.FactorDataType:
			minipack_func_name = "minipack_unpack_int"
		case core.FloatDataType:
			minipack_func_name = "minipack_unpack_double"
		case core.BooleanDataType:
			minipack_func_name = "minipack_unpack_bool"
		}

		field := m.structgep(m.load(event, ""), decl.Index(), "")
		m.store(m.builder.CreateCall(m.module.NamedFunction(minipack_func_name), []llvm.Value{m.load(ptr, ""), sz}, ""), field)
		m.store(m.builder.CreateGEP(m.load(ptr, ""), []llvm.Value{m.load(sz, "")}, ""), ptr)
		m.condbr(m.builder.CreateICmp(llvm.IntNE, m.load(sz, ""), llvm.ConstInt(m.context.Int64Type(), 0, false), ""), loop, loop_skip)
	}

	// loop_skip:
	//     sz = minipack_sizeof_elem_and_data(ptr);
	//     ptr += sz;
	//     if(sz != 0) goto loop else goto error;
	m.builder.SetInsertPointAtEnd(loop_skip)
	m.store(m.builder.CreateCall(m.module.NamedFunction("minipack_sizeof_elem_and_data"), []llvm.Value{m.load(ptr, "")}, ""), sz)
	m.store(m.builder.CreateGEP(m.load(ptr, ""), []llvm.Value{m.load(sz, "")}, ""), ptr)
	m.condbr(m.builder.CreateICmp(llvm.IntNE, m.load(sz, ""), llvm.ConstInt(m.context.Int64Type(), 0, false), ""), loop, error_lbl)

	// error:
	//     return -1;
	m.builder.SetInsertPointAtEnd(error_lbl)
	m.ret(m.constint(-1))

	// exit:
	//     return 0;
	m.builder.SetInsertPointAtEnd(exit)
	m.ret(m.constint(0))

	return fn
}
