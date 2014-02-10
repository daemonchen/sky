package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/codegen/hashmap"
	"github.com/skydb/sky/query/codegen/mapper"
	"github.com/skydb/sky/query/codegen/reducer"
	"github.com/skydb/sky/query/parser"
)

func testevent(timestamp string, args ...interface{}) *db.Event {
	e := &db.Event{Timestamp: musttime(timestamp)}
	e.Data = make(map[int64]interface{})
	for i := 0; i < len(args); i += 2 {
		key := args[i].(int)
		e.Data[int64(key)] = args[i+1]
	}
	return e
}

func musttime(timestamp string) time.Time {
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		panic(err)
	}
	return t
}

func mustmap(value interface{}) map[string]interface{} {
	return value.(map[string]interface{})
}

func mustmarshal(value interface{}) string {
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func debugln(a ...interface{}) (n int, err error) {
	return fmt.Fprintln(os.Stderr, a...)
}

// Executes a query against a multiple shards and return the results.
func withDB(objects map[string][]*db.Event, shardCount int, fn func(db.DB) error) error {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	db := db.New(path, shardCount, false, 4096, 126)
	if err := db.Open(); err != nil {
		debugln("run.mapper.!")
		return err
	}
	defer db.Close()

	// Insert into db.
	if _, err := db.InsertObjects("TBL", objects); err != nil {
		return err
	}

	if err := fn(db); err != nil {
		return err
	}
	return nil
}

// Executes a query against a given set of data and return the results.
func runDBMapper(query string, decls ast.VarDecls, objects map[string][]*db.Event) (*hashmap.Hashmap, error) {
	var h *hashmap.Hashmap
	err := runDBMappers(1, query, decls, objects, func(db db.DB, results []*hashmap.Hashmap) error {
		if len(results) > 0 {
			h = results[0]
		}
		return nil
	})
	return h, err
}

// Executes a query against a multiple shards and return the results.
func runDBMappers(shardCount int, query string, decls ast.VarDecls, objects map[string][]*db.Event, fn func(db.DB, []*hashmap.Hashmap) error) error {
	err := withDB(objects, shardCount, func(db db.DB) error {
		// Retrieve cursors.
		cursors, err := db.Cursors("TBL")
		if err != nil {
			return err
		}
		defer cursors.Close()

		// Create a query.
		q := parser.New().MustParseString(query)
		q.DeclaredVarDecls = append(q.DeclaredVarDecls, decls...)
		q.Finalize()

		// Setup factor test data.
		f, err := db.Factorizer("TBL")
		if err != nil {
			return err
		}
		f.Factorize("action", "A0", true)
		f.Factorize("action", "A1", true)
		f.Factorize("factorVariable", "XXX", true)
		f.Factorize("factorVariable", "YYY", true)

		// Create a mapper generated from the query.
		m, err := mapper.New(q, f)
		if err != nil {
			return err
		}
		// m.Dump()

		// Execute the mappers.
		results := make([]*hashmap.Hashmap, 0)
		for _, cursor := range cursors {
			result := hashmap.New()
			if err = m.Map(cursor, "", result); err != nil {
				return err
			}
			results = append(results, result)
		}

		if err := fn(db, results); err != nil {
			return err
		}
		return nil
	})

	return err
}

// Executes a query against a given set of data, reduces it and return the reduced results.
func runDBMapReducer(shardCount int, query string, decls ast.VarDecls, objects map[string][]*db.Event) (map[string]interface{}, error) {
	var output map[string]interface{}

	// Create a query.
	q := parser.New().MustParseString(query)
	q.DeclaredVarDecls = append(q.DeclaredVarDecls, decls...)
	q.Finalize()

	err := runDBMappers(shardCount, query, decls, objects, func(db db.DB, results []*hashmap.Hashmap) error {
		f, err := db.Factorizer("TBL")
		if err != nil {
			return err
		}

		r := reducer.New(q, f)
		for _, result := range results {
			if err := r.Reduce(result); err != nil {
				return err
			}
		}
		output = r.Output()
		return nil
	})

	if err != nil {
		return nil, err
	}
	return output, nil
}
