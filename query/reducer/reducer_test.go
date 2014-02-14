package reducer_test

import (
"encoding/json"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/hashmap"
	"github.com/skydb/sky/query/mapper"
	"github.com/skydb/sky/query/parser"
	"github.com/skydb/sky/query/reducer"
	"github.com/stretchr/testify/assert"
)

func TestReducerSelectCount(t *testing.T) {
	query := `
		FOR EACH EVENT
			SELECT count()
		END
	`
	result, err := runDBMapReducer(1, query, ast.VarDecls{
		ast.NewVarDecl(8, "foo", "integer"),
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("2000-01-01T00:00:00Z", 1, 10),
			testevent("2000-01-01T00:00:02Z", 1, 20),
		},
		"bar": []*db.Event{
			testevent("2000-01-01T00:00:00Z", 1, 40),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, `{"count":3}`, mustmarshal(result))
}

func TestReducerSelectGroupBy(t *testing.T) {
	query := `
		FOR EACH EVENT
			SELECT sum(integerValue) AS intsum GROUP BY action, booleanValue
		END
	`
	result, err := runDBMapReducer(1, query, ast.VarDecls{
		ast.NewVarDecl(1, "action", "factor"),
		ast.NewVarDecl(2, "booleanValue", "boolean"),
		ast.NewVarDecl(3, "integerValue", "integer"),
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("2000-01-01T00:00:00Z", 1, 1, 2, true, 3, 10),   // A0/true/10
			testevent("2000-01-01T00:00:01Z", 1, 1, 2, false, 3, 20),  // A0/false/20
			testevent("2000-01-01T00:00:02Z", 1, 2, 2, false, 3, 100), // A1/false/100
		},
		"bar": []*db.Event{
			testevent("2000-01-01T00:00:00Z", 1, 1, 2, true, 3, 40), // A0/true/40
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, `{"action":{"A0":{"booleanValue":{"false":{"intsum":20},"true":{"intsum":50}}},"A1":{"booleanValue":{"false":{"intsum":100}}}}}`, mustmarshal(result))
}

func TestReducerSelectInto(t *testing.T) {
	query := `
		FOR EACH EVENT
			SELECT count() INTO "mycount"
		END
	`
	result, err := runDBMapReducer(1, query, ast.VarDecls{
		ast.NewVarDecl(8, "foo", "integer"),
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("2000-01-01T00:00:00Z", 1, 10),
			testevent("2000-01-01T00:00:02Z", 1, 20),
		},
		"bar": []*db.Event{
			testevent("2000-01-01T00:00:00Z", 1, 40),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, `{"mycount":{"count":3}}`, mustmarshal(result))
}

func testevent(timestamp string, args ...interface{}) *db.Event {
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		panic(err)
	}
	e := &db.Event{Timestamp: t}
	e.Data = make(map[int64]interface{})
	for i := 0; i < len(args); i += 2 {
		key := args[i].(int)
		e.Data[int64(key)] = args[i+1]
	}
	return e
}

func mustmarshal(value interface{}) string {
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// Executes a query against a multiple shards and return the results.
func withDB(objects map[string][]*db.Event, shardCount int, fn func(*db.DB) error) error {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	db := &db.DB{}
	if err := db.Open(path, shardCount); err != nil {
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

// Executes a query against a multiple shards and return the results.
func runDBMappers(shardCount int, query string, decls ast.VarDecls, objects map[string][]*db.Event, fn func(*db.DB, []*hashmap.Hashmap) error) error {
	err := withDB(objects, shardCount, func(db *db.DB) error {
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

	err := runDBMappers(shardCount, query, decls, objects, func(db *db.DB, results []*hashmap.Hashmap) error {
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
