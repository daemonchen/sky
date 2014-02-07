package server

import (
	"fmt"
	"sync"

	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/ast/validator"
	"github.com/skydb/sky/query/codegen/hashmap"
	"github.com/skydb/sky/query/codegen/mapper"
	"github.com/skydb/sky/query/codegen/reducer"
	"github.com/skydb/sky/query/parser"
	//"github.com/szferi/gomdb"
)

// queryHandler handles the execute of queries against database tables.
type queryHandler struct {
	s *Server
}

// installQueryHandler adds query routes to the server.
func installQueryHandler(s *Server) *queryHandler {
	h := &queryHandler{s: s}
	s.HandleFunc("/tables/{table}/query", EnsureTableHandler(HandleFunc(h.query))).Methods("POST")
	s.HandleFunc("/tables/{table}/stats", EnsureTableHandler(HandleFunc(h.stats))).Methods("GET")
	return h
}

// query reads the incoming query and executes it against the given table.
func (h *queryHandler) query(s *Server, req Request) (interface{}, error) {
	var data map[string]interface{}
	switch d := req.Data().(type) {
	case map[string]interface{}:
		data = d
	case []byte:
		data = map[string]interface{}{"query": string(d)}
	default:
		return nil, fmt.Errorf("server: map or string input required")
	}
	querystring, _ := data["query"].(string)
	// warn(querystring)
	return h.execute(s, req, querystring)
}

// stats executes a simple event count against the given table.
func (h *queryHandler) stats(s *Server, req Request) (interface{}, error) {
	return h.execute(s, req, "SELECT count()")
}

// execute runs a query against the table.
func (h *queryHandler) execute(s *Server, req Request, querystring string) (interface{}, error) {
	var wg sync.WaitGroup
	t := req.Table()
	t0 := bench("query")

	var data, ok = req.Data().(map[string]interface{})
	if !ok {
		data = make(map[string]interface{})
	}

	// Retrieve prefix.
	prefix, ok := data["prefix"].(string)
	if !ok {
		prefix = req.Var("prefix")
	}

	// Parse query.
	q, err := parser.ParseString(querystring)
	if err != nil {
		return nil, err
	}
	q.DynamicDecl = func(ref *ast.VarRef) *ast.VarDecl {
		p, _ := t.GetPropertyByName(ref.Name)
		if p == nil {
			return nil
		}
		return ast.NewVarDecl(p.Id, p.Name, p.DataType)
	}
	ast.Normalize(q)
	if err := q.Finalize(); err != nil {
		return nil, err
	}

	t0 = bench("query.parse", t0)

	// Validate query.
	if err := validator.Validate(q); err != nil {
		return nil, err
	}

	t0 = bench("query.validate", t0)

	// Retrieve factorizer and database cursors.
	f, err := s.db.Factorizer(t.Name)
	if err != nil {
		return nil, err
	}
	cursors, err := s.db.Cursors(t.Name)
	if err != nil {
		return nil, err
	}
	defer cursors.Close()

	// TODO(benbjohnson): Add Mapper.Clone() and run each shard separately.

	// Generate mapper code.
	m, err := mapper.New(q, f)
	if err != nil {
		return nil, err
	}
	defer m.Close()
	// m.Dump()

	t0 = bench("query.codegen", t0)

	m.Iterate(cursors[0])
	t0 = bench("query.iterate", t0)

	// Execute one mapper for each cursor.
	t1 := bench("map")
	count := len(cursors)
	results := make(chan interface{}, count)
	for _, cursor := range cursors {
		wg.Add(1)
		//go func(cursor *mdb.Cursor) {
			result := hashmap.New()
			if err := m.Map(cursor, prefix, result); err == nil {
				results <- result
			} else {
				results <- err
			}
			bench("map", t1)
			wg.Done()
		//}(cursor)
	}

	// Don't exit function until all mappers finish.
	defer wg.Wait()

	// Close results channel after all mappers are done.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Combine all the results into one final result.
	err = nil
	r := reducer.New(q, f)
	for result := range results {
		switch result := result.(type) {
		case *hashmap.Hashmap:
			if err := r.Reduce(result); err != nil {
				return nil, err
			}
		case error:
			return nil, result
		}
	}

	return r.Output(), nil
}
