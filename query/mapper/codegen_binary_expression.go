package mapper

import (
	"fmt"
	"github.com/axw/gollvm/llvm"
	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
)

func (m *Mapper) codegenBinaryExpression(node *ast.BinaryExpression, event llvm.Value, tbl *ast.Symtable) (llvm.Value, error) {
	// Special handling for factor expressions.
	if m.isFactorVarRef(node.LHS, tbl) {
		return m.codegenFactorEquality(node, node.LHS.(*ast.VarRef), node.RHS, event, tbl)
	} else if m.isFactorVarRef(node.RHS, tbl) {
		return m.codegenFactorEquality(node, node.RHS.(*ast.VarRef), node.LHS, event, tbl)
	}

	lhs, err := m.codegenExpression(node.LHS, event, tbl)
	if err != nil {
		return nilValue, err
	}

	rhs, err := m.codegenExpression(node.RHS, event, tbl)
	if err != nil {
		return nilValue, err
	}

	// Make operands equal types.
	var fp bool
	lhs, rhs, fp = m.promoteOperands(lhs, rhs)

	switch node.Op {
	case ast.OpEquals:
		if fp {
			return m.fcmp(llvm.FloatUEQ, lhs, rhs, ""), nil
		} else {
			return m.icmp(llvm.IntEQ, lhs, rhs, ""), nil
		}
	case ast.OpNotEquals:
		if fp {
			return m.fcmp(llvm.FloatUNE, lhs, rhs, ""), nil
		} else {
			return m.icmp(llvm.IntNE, lhs, rhs, ""), nil
		}
	case ast.OpGreaterThan:
		if fp {
			return m.fcmp(llvm.FloatUGT, lhs, rhs, ""), nil
		} else {
			return m.icmp(llvm.IntSGT, lhs, rhs, ""), nil
		}
	case ast.OpGreaterThanOrEqualTo:
		if fp {
			return m.fcmp(llvm.FloatUGE, lhs, rhs, ""), nil
		} else {
			return m.icmp(llvm.IntSGE, lhs, rhs, ""), nil
		}
	case ast.OpLessThan:
		if fp {
			return m.fcmp(llvm.FloatULT, lhs, rhs, ""), nil
		} else {
			return m.icmp(llvm.IntSLT, lhs, rhs, ""), nil
		}
	case ast.OpLessThanOrEqualTo:
		if fp {
			return m.fcmp(llvm.FloatULE, lhs, rhs, ""), nil
		} else {
			return m.icmp(llvm.IntSLE, lhs, rhs, ""), nil
		}
	case ast.OpPlus:
		if fp {
			return m.builder.CreateFAdd(lhs, rhs, ""), nil
		} else {
			return m.builder.CreateAdd(lhs, rhs, ""), nil
		}
	case ast.OpMinus:
		if fp {
			return m.builder.CreateFSub(lhs, rhs, ""), nil
		} else {
			return m.builder.CreateSub(lhs, rhs, ""), nil
		}
	case ast.OpMultiply:
		if fp {
			return m.builder.CreateFMul(lhs, rhs, ""), nil
		} else {
			return m.builder.CreateMul(lhs, rhs, ""), nil
		}
	case ast.OpDivide:
		if fp {
			return m.builder.CreateFDiv(lhs, rhs, ""), nil
		} else {
			return m.builder.CreateSDiv(lhs, rhs, ""), nil
		}
	case ast.OpAnd:
		return m.builder.CreateAnd(lhs, rhs, ""), nil
	case ast.OpOr:
		return m.builder.CreateOr(lhs, rhs, ""), nil
	default:
	}

	return nilValue, fmt.Errorf("Invalid binary expression operator: %d", node.Op)
}

func (m *Mapper) isFactorVarRef(node ast.Expression, tbl *ast.Symtable) bool {
	ref, ok := node.(*ast.VarRef)
	if !ok {
		return false
	}
	decl := tbl.Find(ref.Name)
	return (decl != nil && decl.DataType == db.Factor)
}

func (m *Mapper) promoteOperands(lhs, rhs llvm.Value) (llvm.Value, llvm.Value, bool) {
	lhsTypeKind := lhs.Type().TypeKind()
	rhsTypeKind := rhs.Type().TypeKind()

	if lhsTypeKind == llvm.DoubleTypeKind && rhsTypeKind == llvm.IntegerTypeKind {
		rhs = m.sitofp(rhs)
	} else if lhsTypeKind == llvm.DoubleTypeKind && rhsTypeKind == llvm.IntegerTypeKind {
		lhs = m.sitofp(lhs)
	}

	fp := (lhsTypeKind == llvm.DoubleTypeKind || rhsTypeKind == llvm.DoubleTypeKind)
	return lhs, rhs, fp
}

func (m *Mapper) codegenFactorEquality(node *ast.BinaryExpression, lhs *ast.VarRef, rhs ast.Expression, event llvm.Value, tbl *ast.Symtable) (llvm.Value, error) {
	// Only allow == or != for factors.
	var op llvm.IntPredicate
	switch node.Op {
	case ast.OpEquals:
		op = llvm.IntEQ
	case ast.OpNotEquals:
		op = llvm.IntNE
	default:
		return nilValue, fmt.Errorf("Non-equality operators not allowed for factor data types: %s", node.String())
	}

	lhsValue, err := m.codegenExpression(lhs, event, tbl)
	if err != nil {
		return nilValue, err
	}

	// Only allow comparisons to strings or other associated factors.
	switch rhs := rhs.(type) {
	case *ast.StringLiteral:
		decl := tbl.Find(lhs.Name)
		name := decl.Association
		if name == "" {
			name = decl.Name
		}

		// Find property on table.
		p, err := m.table.Property(name)
		if err != nil {
			return nilValue, err
		} else if p == nil {
			return nilValue, &Error{"variable not found: " + name, nil}
		}

		// Factorize value.
		id, err := p.Factorize(rhs.Value)
		if err != nil {
			return nilValue, err
		}
		return m.icmp(op, lhsValue, m.constint(int(id)), ""), nil

	case *ast.VarRef:
		rhsValue, err := m.codegenExpression(rhs, event, tbl)
		if err != nil {
			return nilValue, err
		}
		return m.icmp(op, lhsValue, rhsValue, ""), nil

	default:
		return nilValue, fmt.Errorf("Invalid factor expression: %s", node.String())
	}
}
