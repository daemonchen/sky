package query

import (
	"github.com/skydb/sky/core"
	"github.com/skydb/sky/db"
	"strconv"
)

type StringLiteral struct {
	queryElementImpl
	value string
}

func (l *StringLiteral) VarRefs() []*VarRef {
	return []*VarRef{}
}

func (l *StringLiteral) Codegen() (string, error) {
	// Retrieve the opposite side of the binary expression this literal is a part of.
	var opposite Expression
	if parent, ok := l.Parent().(*BinaryExpression); ok {
		if parent.Lhs() == l {
			opposite = parent.Rhs()
		} else if parent.Rhs() == l {
			opposite = parent.Lhs()
		}
	}

	// If the string is a part of a binary expression where the other side
	// is a factor variable reference then we have to convert this value to
	// a factor.
	if ref, ok := opposite.(*VarRef); ok {
		query := l.Query()
		variable, err := ref.Variable()
		if err != nil {
			return "", err
		}
		if variable.DataType == core.FactorDataType {
			targetName := variable.Name
			if variable.Association != "" {
				targetName = variable.Association
			}
			sequence, err := query.factorizer.Factorize(targetName, l.value, false)
			if _, ok := err.(*db.FactorNotFound); ok {
				return "0", nil
			} else if err != nil {
				return "", err
			} else {
				return strconv.FormatUint(sequence, 10), nil
			}
		}
	}

	return l.String(), nil
}

func (l *StringLiteral) String() string {
	return strconv.Quote(l.value)
}
