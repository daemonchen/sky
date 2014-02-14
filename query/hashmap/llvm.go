package hashmap

import (
	"github.com/axw/gollvm/llvm"
)

func DeclareType(m llvm.Module, c llvm.Context) llvm.Type {
	return c.StructCreateNamed("sky_hashmap")
}

func Declare(m llvm.Module, c llvm.Context, hashmapType llvm.Type) {
	llvm.AddFunction(m, "sky_hashmap_new", llvm.FunctionType(llvm.PointerType(hashmapType, 0), []llvm.Type{}, false))
	llvm.AddFunction(m, "sky_hashmap_free", llvm.FunctionType(c.VoidType(), []llvm.Type{llvm.PointerType(hashmapType, 0)}, false))
	llvm.AddFunction(m, "sky_hashmap_get", llvm.FunctionType(c.Int64Type(), []llvm.Type{llvm.PointerType(hashmapType, 0), c.Int64Type()}, false))
	llvm.AddFunction(m, "sky_hashmap_set", llvm.FunctionType(c.VoidType(), []llvm.Type{llvm.PointerType(hashmapType, 0), c.Int64Type(), c.Int64Type()}, false))
	llvm.AddFunction(m, "sky_hashmap_get_double", llvm.FunctionType(c.DoubleType(), []llvm.Type{llvm.PointerType(hashmapType, 0), c.Int64Type()}, false))
	llvm.AddFunction(m, "sky_hashmap_set_double", llvm.FunctionType(c.VoidType(), []llvm.Type{llvm.PointerType(hashmapType, 0), c.Int64Type(), c.DoubleType()}, false))
	llvm.AddFunction(m, "sky_hashmap_submap", llvm.FunctionType(llvm.PointerType(hashmapType, 0), []llvm.Type{llvm.PointerType(hashmapType, 0), c.Int64Type()}, false))
}
