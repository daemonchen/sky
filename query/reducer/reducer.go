package reducer

import (
	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query"
	"github.com/skydb/sky/query/ast"
)

// Reducer takes the results of multiple mapper executions and combines
// them into a single final output.
type Reducer struct {
	table  *db.Table
	query  *ast.Query
	output map[string]interface{}
}

// New creates a new Reducer instance.
func New(q *ast.Query, t *db.Table) *Reducer {
	return &Reducer{
		table:  t,
		query:  q,
		output: make(map[string]interface{}),
	}
}

// Output returns the final reduced output.
func (r *Reducer) Output() map[string]interface{} {
	return r.output
}

// Reduce executes the reducer against a hashmap returned from a Mapper.
func (r *Reducer) Reduce(h *query.Hashmap) error {
	return r.reduceQuery(r.query, h)
}
