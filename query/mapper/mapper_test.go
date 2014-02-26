package mapper_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/hashmap"
	"github.com/skydb/sky/query/mapper"
	"github.com/skydb/sky/query/parser"
	"github.com/stretchr/testify/assert"
)

var (
	HASH_EOF       = int64(hashmap.String("@eof"))
	HASH_EOS       = int64(hashmap.String("@eos"))
	HASH_ACTION    = int64(hashmap.String("action"))
	HASH_FOO       = int64(hashmap.String("foo"))
	HASH_COUNT     = int64(hashmap.String("count"))
	HASH_SUM_MYVAR = int64(hashmap.String("sum_myVar"))
)

func TestMapperSelectCount(t *testing.T) {
	query := `
		FOR EACH EVENT
			SELECT count()
		END
	`
	result, err := runDBMapper(query, []*db.Property{
		{Name:"foo", DataType:db.Integer, Transient: false},
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
	if assert.NotNil(t, result) {
		assert.Equal(t, result.Get(HASH_COUNT), 3)
	}
}

func TestMapperSelectInto(t *testing.T) {
	query := `
		FOR EACH EVENT
			SELECT count() INTO "foo"
		END
	`
	result, err := runDBMapper(query, []*db.Property{
		{Name:"foo", DataType:db.Integer, Transient: false},
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
	if assert.NotNil(t, result) {
		assert.Equal(t, result.Submap(HASH_FOO).Get(HASH_COUNT), 3)
	}
}

func TestMapperCondition(t *testing.T) {
	query := `
		FOR EACH EVENT
			WHEN foo == 10 THEN
				SELECT count()
			END
		END
	`
	result, err := runDBMapper(query, []*db.Property{
		{Name:"foo", DataType:db.Integer, Transient: false},
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
	if assert.NotNil(t, result) {
		assert.Equal(t, result.Get(HASH_COUNT), 1)
	}
}

func TestMapperFactorEquality(t *testing.T) {
	query := `
		FOR EACH EVENT
			WHEN factorVariable == "XXX" THEN
				SELECT count()
			END
		END
	`
	result, err := runDBMapper(query, []*db.Property{
		{Name:"factorVariable", DataType:db.Factor, Transient: false},
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("2000-01-01T00:00:00Z", "factorVariable", "XXX"), // "XXX"
			testevent("2000-01-01T00:00:02Z", "factorVariable", "YYY"), // "YYY"
		},
		"bar": []*db.Event{
			testevent("2000-01-01T00:00:00Z", "factorVariable", "XXX"), // "XXX"
		},
	})
	assert.NoError(t, err)
	if assert.NotNil(t, result) {
		assert.Equal(t, result.Get(HASH_COUNT), 2)
	}
}

func TestMapperAssignment(t *testing.T) {
	query := `
		DECLARE myVar AS INTEGER
		FOR EACH EVENT
			SET myVar = myVar + 1
			SELECT sum(myVar)
		END
	`
	result, err := runDBMapper(query, []*db.Property{
		{Name:"integerVariable", DataType:db.Integer, Transient: false},
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("2000-01-01T00:00:00Z", "integerVariable", 1),
			testevent("2000-01-01T00:00:02Z", "integerVariable", 2),
		},
		"bar": []*db.Event{
			testevent("2000-01-01T00:00:00Z", "integerVariable", 3),
		},
	})
	assert.NoError(t, err)
	if assert.NotNil(t, result) {
		assert.Equal(t, result.Get(HASH_SUM_MYVAR), 4)
	}
}

func TestMapperSessionLoop(t *testing.T) {
	var h *hashmap.Hashmap
	query := `
		FOR EACH SESSION DELIMITED BY 2 HOURS
		  FOR EACH EVENT
		    SELECT count() GROUP BY action, @@eof, @@eos
		  END
		END
	`
	result, err := runDBMapper(query, []*db.Property{
		{Name:"action", DataType:db.Factor, Transient: false},
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("1970-01-01T00:00:01Z", "action", "A0"), // ts=1
			testevent("1970-01-01T01:59:59Z", "action", "A1"), // ts=7199
			testevent("1970-01-02T00:00:00Z", "action", "A0"), // ts=86400
			testevent("1970-01-02T02:00:00Z", "action", "A1"), // ts=93600
		},

		"bar": []*db.Event{
			testevent("1970-01-02T02:00:00Z", "action", "A0"), // action=A0
		},
	})
	assert.NoError(t, err)
	if assert.NotNil(t, result) {
		// action=A0
		h = result.Submap(HASH_ACTION).Submap(1)
		assert.Equal(t, h.Submap(HASH_EOF).Submap(0).Submap(HASH_EOS).Submap(0).Get(HASH_COUNT), 1) // A0 eof=0 eos=0 count()
		assert.Equal(t, h.Submap(HASH_EOF).Submap(0).Submap(HASH_EOS).Submap(1).Get(HASH_COUNT), 1) // A0 eof=0 eos=1 count()
		assert.Equal(t, h.Submap(HASH_EOF).Submap(1).Submap(HASH_EOS).Submap(0).Get(HASH_COUNT), 0) // A0 eof=1 eos=0 count()
		assert.Equal(t, h.Submap(HASH_EOF).Submap(1).Submap(HASH_EOS).Submap(1).Get(HASH_COUNT), 1) // A0 eof=1 eos=1 count()

		// action=A1
		h = result.Submap(HASH_ACTION).Submap(2)
		assert.Equal(t, h.Submap(HASH_EOF).Submap(0).Submap(HASH_EOS).Submap(0).Get(HASH_COUNT), 0) // A0 eof=0 eos=0 count()
		assert.Equal(t, h.Submap(HASH_EOF).Submap(0).Submap(HASH_EOS).Submap(1).Get(HASH_COUNT), 1) // A0 eof=0 eos=1 count()
		assert.Equal(t, h.Submap(HASH_EOF).Submap(1).Submap(HASH_EOS).Submap(0).Get(HASH_COUNT), 0) // A0 eof=1 eos=0 count()
		assert.Equal(t, h.Submap(HASH_EOF).Submap(1).Submap(HASH_EOS).Submap(1).Get(HASH_COUNT), 1) // A0 eof=1 eos=1 count()
	}
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

// Executes a query against a given set of data and return the results.
func runDBMapper(query string, properties []*db.Property, objects map[string][]*db.Event) (*hashmap.Hashmap, error) {
	var h *hashmap.Hashmap
	err := runDBMappers(1, query, properties, objects, func(table *db.Table, results []*hashmap.Hashmap) error {
		if len(results) > 0 {
			h = results[0]
		}
		return nil
	})
	return h, err
}

// Executes a query against a multiple shards and return the results.
func runDBMappers(shardCount int, query string, properties []*db.Property, objects map[string][]*db.Event, fn func(*db.Table, []*hashmap.Hashmap) error) error {
	err := withTable(properties, objects, shardCount, func(table *db.Table) error {
		// Create a query.
		q := parser.New().MustParseString(query)
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
		results := make([]*hashmap.Hashmap, 0)
		table.ForEachShard(func (c *db.Cursor) {
			result := hashmap.New()
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
