package reducer_test

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query"
	"github.com/skydb/sky/query/ast"
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
	result, err := runDBMapReducer(1, query, []*db.Property{
		{Name: "foo", DataType: db.Integer, Transient: false},
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("2000-01-01T00:00:00Z", "foo", 10),
			testevent("2000-01-01T00:00:02Z", "foo", 20),
		},
		"bar": []*db.Event{
			testevent("2000-01-01T00:00:00Z", "foo", 40),
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
	result, err := runDBMapReducer(1, query, []*db.Property{
		{Name: "action", DataType: db.Factor, Transient: false},
		{Name: "booleanValue", DataType: db.Boolean, Transient: false},
		{Name: "integerValue", DataType: db.Integer, Transient: false},
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("2000-01-01T00:00:00Z", "action", "A0", "booleanValue", true, "integerValue", 10),
			testevent("2000-01-01T00:00:01Z", "action", "A0", "booleanValue", false, "integerValue", 20),
			testevent("2000-01-01T00:00:02Z", "action", "A1", "booleanValue", false, "integerValue", 100),
		},
		"bar": []*db.Event{
			testevent("2000-01-01T00:00:00Z", "action", "A0", "booleanValue", true, "integerValue", 40),
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
	result, err := runDBMapReducer(1, query, []*db.Property{
		{Name: "foo", DataType: db.Integer, Transient: false},
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("2000-01-01T00:00:00Z", "foo", 10),
			testevent("2000-01-01T00:00:02Z", "foo", 20),
		},
		"bar": []*db.Event{
			testevent("2000-01-01T00:00:00Z", "foo", 40),
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
	e.Data = make(map[string]interface{})
	for i := 0; i < len(args); i += 2 {
		key := args[i].(string)
		e.Data[key] = args[i+1]
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
func withTable(properties []*db.Property, objects map[string][]*db.Event, shardCount int, fn func(*db.Table) error) error {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	db := &db.DB{}
	if err := db.Open(path); err != nil {
		return err
	}
	defer db.Close()

	// Create table.
	table, err := db.CreateTable("TBL", shardCount)
	if err != nil {
		panic("cannot create table: " + err.Error())
	}

	// Create properties.
	for _, property := range properties {
		p, err := table.CreateProperty(property.Name, property.DataType, property.Transient)
		if err != nil {
			panic("create property error: " + err.Error())
		}
		property.ID = p.ID
	}

	// Insert data.
	if err := table.InsertObjects(objects); err != nil {
		return err
	}

	if err := fn(table); err != nil {
		return err
	}
	return nil
}

// Executes a query against a multiple shards and return the results.
func runDBMappers(shardCount int, querystring string, properties []*db.Property, objects map[string][]*db.Event, fn func(*db.Table, []*query.Hashmap) error) error {
	err := withTable(properties, objects, shardCount, func(table *db.Table) error {
		// Create a query.
		q := parser.New().MustParseString(querystring)
		for _, property := range properties {
			q.DeclaredVarDecls = append(q.DeclaredVarDecls, ast.NewVarDecl(property.ID, property.Name, property.DataType))
		}
		q.Finalize()

		// Create a mapper generated from the query.
		m, err := mapper.New(q, table)
		if err != nil {
			return err
		}
		// m.Dump()

		// Execute the mappers.
		results := make([]*query.Hashmap, 0)
		table.ForEach(func(c *db.Cursor) {
			result := query.NewHashmap()
			if err = m.Map(c, "", result); err != nil {
				panic("map error: " + err.Error())
			}
			results = append(results, result)
		})

		if err := fn(table, results); err != nil {
			return err
		}
		return nil
	})

	return err
}

// Executes a query against a given set of data, reduces it and return the reduced results.
func runDBMapReducer(shardCount int, querystring string, properties []*db.Property, objects map[string][]*db.Event) (map[string]interface{}, error) {
	var output map[string]interface{}

	// Create a query.
	q := parser.New().MustParseString(querystring)
	for _, property := range properties {
		q.DeclaredVarDecls = append(q.DeclaredVarDecls, ast.NewVarDecl(property.ID, property.Name, property.DataType))
	}
	q.Finalize()

	err := runDBMappers(shardCount, querystring, properties, objects, func(table *db.Table, results []*query.Hashmap) error {
		r := reducer.New(q, table)
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
