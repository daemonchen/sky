package server

import (
	"fmt"
	"sync"

	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query"
	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/ast/validator"
	"github.com/skydb/sky/query/mapper"
	"github.com/skydb/sky/query/parser"
	"github.com/skydb/sky/query/reducer"
)

// queryHandler handles the execute of queries against database tables.
type queryHandler struct {
	s *Server
}

// installQueryHandler adds query routes to the server.
func installQueryHandler(s *Server) *queryHandler {
	h := &queryHandler{s: s}
	s.HandleFunc("/tables/{table}/query", EnsureTableHandler(HandleFunc(h.query))).Methods("POST")
	s.HandleFunc("/tables/{table}/count", EnsureTableHandler(HandleFunc(h.count))).Methods("GET")
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
		return nil, fmt.Errorf("map or string input required")
	}
	querystring, _ := data["query"].(string)
	// warn(querystring)
	return h.execute(s, req, querystring)
}

// count executes a simple event count against the given table.
func (h *queryHandler) count(s *Server, req Request) (interface{}, error) {
	return h.execute(s, req, "SELECT count()")
}

// execute runs a query against the table.
func (h *queryHandler) execute(s *Server, req Request, querystring string) (interface{}, error) {
	var wg sync.WaitGroup
	t := req.Table()

	var data, ok = req.Data().(map[string]interface{})
	if !ok {
		data = make(map[string]interface{})
	}

	// Retrieve prefix.
	prefix, ok := data["prefix"].(string)
	if !ok {
		prefix = req.Var("prefix")
	}
	/*
		if prefix == "" {
			return nil, fmt.Errorf("prefix required")
		}
	*/

	// Parse query.
	q, err := parser.ParseString(querystring)
	if err != nil {
		return nil, err
	}
	q.DynamicDecl = func(ref *ast.VarRef) *ast.VarDecl {
		p, _ := t.Property(ref.Name)
		if p == nil {
			return nil
		}
		return ast.NewVarDecl(p.ID, p.Name, p.DataType)
	}
	ast.Normalize(q)
	if err := q.Finalize(); err != nil {
		return nil, err
	}

	// Validate query.
	if err := validator.Validate(q); err != nil {
		return nil, err
	}

	// TODO(benbjohnson): Add Mapper.Clone() and run each shard separately.

	// Generate mapper code.
	m, err := mapper.New(q, t)
	if err != nil {
		return nil, err
	}
	defer m.Close()
	// m.Dump()

	// Execute one mapper for each cursor.
	results := make(chan interface{}, t.ShardCount())
	t.ForEach(func(cursor *db.Cursor) {
		wg.Add(1)
		go func(cursor *db.Cursor) {
			defer cursor.Close()
			result := query.NewHashmap()
			if err := m.Map(cursor, prefix, result); err == nil {
				results <- result
			} else {
				results <- err
			}
			wg.Done()
		}(cursor)
	})

	// Don't exit function until all mappers finish.
	defer wg.Wait()

	// Close results channel after all mappers are done.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Combine all the results into one final result.
	err = nil
	r := reducer.New(q, t)
	for result := range results {
		switch result := result.(type) {
		case *query.Hashmap:
			if err := r.Reduce(result); err != nil {
				return nil, err
			}
		case error:
			return nil, result
		}
	}

	return r.Output(), nil
}
