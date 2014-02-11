package ast

import (
	"fmt"
	"sort"
	"strings"

	"github.com/skydb/sky/db"
)

// Query represents a collection of statements used to process data in
// the Sky database.
type Query struct {
	Prefix           string
	DynamicDecl      func(*VarRef) *VarDecl
	SystemVarDecls   VarDecls
	DeclaredVarDecls VarDecls
	Statements       Statements
	dynamicVarDecls  VarDecls
	decls            VarDecls
	finalized        bool
}

func (q *Query) node()  {}
func (q *Query) block() {}

// NewQuery returns a new query.
func NewQuery() *Query {
	q := &Query{}
	q.SystemVarDecls = VarDecls{
		NewVarDecl(0, "@eof", db.Boolean),
		NewVarDecl(0, "@eos", db.Boolean),
		NewVarDecl(0, "@timestamp", db.Integer),
	}
	return q
}

// Finalize performs final processing on a query to resolve dynamically declared
// variables and index the variable declarations.
func (q *Query) Finalize() error {
	if q.Finalized() {
		return nil
	}

	// Resolve undeclared variable references.
	for _, ref := range FindUndeclaredRefs(q) {
		var decl *VarDecl
		if q.DynamicDecl != nil {
			decl = q.DynamicDecl(ref)
		}
		if decl == nil {
			return fmt.Errorf("query: undeclared variable: %s", ref.Name)
		}
		q.dynamicVarDecls = append(q.dynamicVarDecls, decl)
	}

	// Generate variable declaration indices.
	if err := q.finalizeVarDecls(); err != nil {
		return err
	}

	q.finalizeFields()
	q.finalized = true

	return nil
}

// finalizeVarDecls assigns indices to variable declarations.
func (q *Query) finalizeVarDecls() error {
	var err error
	if q.decls, err = FindVarDecls(q); err != nil {
		return err
	}
	sort.Sort(q.decls)
	for i, decl := range q.decls {
		decl.index = i
	}
	return nil
}

// finalizeVarDecls assigns indices to variable declarations.
func (q *Query) finalizeFields() {
	var s *Selection
	lookup := make(map[string]bool)
	ForEach(q, func(n Node) {
		switch n := n.(type) {
		case *Selection:
			s = n
		case *Field:
			path := fmt.Sprintf("%s.%s", s.Path(), n.Identifier())
			n.reducible = lookup[path]
			lookup[path] = true
		}
	})
}

// Finalized returns whether the query has been finalized already.
func (q *Query) Finalized() bool {
	return q.finalized
}

// VarDecls returns a list of indexed variable declarations.
func (q *Query) VarDecls() VarDecls {
	return q.decls
}

// Selections returns a list of all selections within the query.
func (q *Query) Selections() []*Selection {
	selections := make([]*Selection, 0)
	ForEach(q, func(node Node) {
		if selection, ok := node.(*Selection); ok {
			selections = append(selections, selection)
		}
	})
	return selections
}

// String returns the query as a string representation.
func (q *Query) String() string {
	arr := []string{}
	for _, v := range q.DeclaredVarDecls {
		arr = append(arr, v.String())
	}
	arr = append(arr, q.Statements.String())
	return strings.Join(arr, "\n")
}
