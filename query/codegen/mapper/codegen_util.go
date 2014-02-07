package mapper

import (
	"github.com/axw/gollvm/llvm"
)

// printf inserts a call to "printf" at the current builder position.
func (m *Mapper) printf(format string, values ...interface{}) llvm.Value {
	vals := []llvm.Value{}
	vals = append(vals, m.builder.CreateGlobalString(format, ""))

	for _, value := range values {
		switch value := value.(type) {
		case string:
			vals = append(vals, m.builder.CreateGlobalString(value, ""))
		case llvm.Value:
			vals = append(vals, value)
		default:
			panic("Invalid argument to printf call!")
		}
	}
	return m.builder.CreateCall(m.module.NamedFunction("printf"), vals, "")
}

// trace inserts a "printf"-style call at the current builder position if the
// Mapper has TraceEnabled set to true.
func (m *Mapper) trace(format string, values ...interface{}) llvm.Value {
	if m.TraceEnabled {
		return m.printf("[trace] "+format+"\n", values...)
	}
	return nilValue
}
