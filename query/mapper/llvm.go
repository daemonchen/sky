package mapper

import (
	"fmt"

	"github.com/axw/gollvm/llvm"
)

var nilValue llvm.Value

func (m *Mapper) alloca(typ llvm.Type, name ...string) llvm.Value {
	return m.builder.CreateAlloca(typ, fname(name))
}

func (m *Mapper) add(lhs llvm.Value, rhs llvm.Value, name ...string) llvm.Value {
	return m.builder.CreateAdd(lhs, rhs, fname(name))
}

func (m *Mapper) and(lhs llvm.Value, rhs llvm.Value, name ...string) llvm.Value {
	return m.builder.CreateAnd(lhs, rhs, fname(name))
}

func (m *Mapper) br(bb llvm.BasicBlock) llvm.Value {
	return m.builder.CreateBr(bb)
}

func (m *Mapper) call(fn interface{}, args ...llvm.Value) llvm.Value {
	if functionName, ok := fn.(string); ok {
		fn = m.module.NamedFunction(functionName)
	}
	return m.builder.CreateCall(fn.(llvm.Value), args, "")
}

func (m *Mapper) condbr(ifv llvm.Value, thenb, elseb llvm.BasicBlock) llvm.Value {
	return m.builder.CreateCondBr(ifv, thenb, elseb)
}

func (m *Mapper) constint(value int) llvm.Value {
	return llvm.ConstIntFromString(m.context.Int64Type(), fmt.Sprintf("%d", value), 10)
}

func (m *Mapper) constfloat(value float64) llvm.Value {
	return llvm.ConstFloat(m.context.DoubleType(), value)
}

func (m *Mapper) sitofp(v llvm.Value, name ...string) llvm.Value {
	return m.builder.CreateSIToFP(v, m.context.DoubleType(), fname(name))
}

func (m *Mapper) fcmp(pred llvm.FloatPredicate, lhs, rhs llvm.Value, name ...string) llvm.Value {
	return m.builder.CreateFCmp(pred, lhs, rhs, fname(name))
}

func (m *Mapper) icmp(pred llvm.IntPredicate, lhs, rhs llvm.Value, name ...string) llvm.Value {
	return m.builder.CreateICmp(pred, lhs, rhs, fname(name))
}

func (m *Mapper) load(value llvm.Value, name ...string) llvm.Value {
	return m.builder.CreateLoad(value, fname(name))
}

func (m *Mapper) event_ref(cursor_ref llvm.Value, name ...string) llvm.Value {
	return m.structgep(m.load(cursor_ref), cursorEventElementIndex)
}

func (m *Mapper) next_event_ref(cursor_ref llvm.Value, name ...string) llvm.Value {
	return m.structgep(m.load(cursor_ref), cursorNextEventElementIndex)
}

func (m *Mapper) load_eof(event_ref llvm.Value, name ...string) llvm.Value {
	return m.load(m.structgep(m.load(event_ref), eventEofElementIndex))
}

func (m *Mapper) load_eos(event_ref llvm.Value, name ...string) llvm.Value {
	return m.load(m.structgep(m.load(event_ref), eventEosElementIndex))
}

func (m *Mapper) or(lhs llvm.Value, rhs llvm.Value, name ...string) llvm.Value {
	return m.builder.CreateAnd(lhs, rhs, fname(name))
}

func (m *Mapper) phi(typ llvm.Type, values []llvm.Value, blocks []llvm.BasicBlock, name ...string) llvm.Value {
	phi := m.builder.CreatePHI(typ, fname(name))
	phi.AddIncoming(values, blocks)
	return phi
}

func (m *Mapper) ptrtype() llvm.Type {
	return llvm.PointerType(m.context.Int8Type(), 0)
}

func (m *Mapper) ptrnull() llvm.Value {
	return llvm.ConstPointerNull(m.ptrtype())
}

func (m *Mapper) ret(value llvm.Value) llvm.Value {
	return m.builder.CreateRet(value)
}

func (m *Mapper) retvoid() llvm.Value {
	return m.builder.CreateRetVoid()
}

func (m *Mapper) store(src llvm.Value, dest llvm.Value) llvm.Value {
	return m.builder.CreateStore(src, dest)
}

func (m *Mapper) structgep(value llvm.Value, index int, name ...string) llvm.Value {
	return m.builder.CreateStructGEP(value, index, fname(name))
}

func (m *Mapper) isEOS(cursor, event, next_event llvm.Value) llvm.Value {
	timestamp := m.load(m.structgep(event, eventTimestampElementIndex), "timestamp")
	nextTimestamp := m.load(m.structgep(next_event, eventTimestampElementIndex), "next_timestamp")
	sessionIdleTime := m.load(m.structgep(m.load(cursor), cursorSessionIdleTimeElementIndex), "session_idle_time")
	maxTimestamp := m.add(timestamp, sessionIdleTime, "max_timestamp")
	return m.and(m.icmp(llvm.IntSGT, sessionIdleTime, m.constint(0)), m.icmp(llvm.IntSLE, maxTimestamp, nextTimestamp))
}

func fname(names []string) string {
	if len(names) > 0 {
		return names[0]
	}
	return ""
}
