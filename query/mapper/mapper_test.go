package mapper_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/parser"
	"github.com/skydb/sky/query/mapper"
	"github.com/skydb/sky/query/hashmap"
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
	result, err := runDBMapper(query, ast.VarDecls{
		ast.NewVarDecl(1, "foo", "integer"),
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
	result, err := runDBMapper(query, ast.VarDecls{
		ast.NewVarDecl(1, "foo", "integer"),
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
	result, err := runDBMapper(query, ast.VarDecls{
		ast.NewVarDecl(1, "foo", "integer"),
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
	result, err := runDBMapper(query, ast.VarDecls{
		ast.NewVarDecl(2, "factorVariable", "factor"),
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("2000-01-01T00:00:00Z", 2, 1), // "XXX"
			testevent("2000-01-01T00:00:02Z", 2, 2), // "YYY"
		},
		"bar": []*db.Event{
			testevent("2000-01-01T00:00:00Z", 2, 1), // "XXX"
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
	result, err := runDBMapper(query, ast.VarDecls{
		ast.NewVarDecl(2, "integerVariable", "integer"),
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("2000-01-01T00:00:00Z", 2, 1), // myVar=1, sum=1
			testevent("2000-01-01T00:00:02Z", 2, 2), // myVar=2, sum=3
		},
		"bar": []*db.Event{
			testevent("2000-01-01T00:00:00Z", 2, 3), // myVar=1, sum=4
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
	result, err := runDBMapper(query, ast.VarDecls{
		ast.NewVarDecl(1, "action", "factor"),
	}, map[string][]*db.Event{
		"foo": []*db.Event{
			testevent("1970-01-01T00:00:01Z", 1, 1), // ts=1,     action=A0
			testevent("1970-01-01T01:59:59Z", 1, 2), // ts=7199,  action=A1
			testevent("1970-01-02T00:00:00Z", 1, 1), // ts=86400, action=A0
			testevent("1970-01-02T02:00:00Z", 1, 2), // ts=93600, action=A1
		},

		"bar": []*db.Event{
			testevent("1970-01-02T02:00:00Z", 1, 1), // action=A0
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
	e.Data = make(map[int64]interface{})
	for i := 0; i < len(args); i += 2 {
		key := args[i].(int)
		e.Data[int64(key)] = args[i+1]
	}
	return e
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

// Executes a query against a given set of data and return the results.
func runDBMapper(query string, decls ast.VarDecls, objects map[string][]*db.Event) (*hashmap.Hashmap, error) {
	var h *hashmap.Hashmap
	err := runDBMappers(1, query, decls, objects, func(db *db.DB, results []*hashmap.Hashmap) error {
		if len(results) > 0 {
			h = results[0]
		}
		return nil
	})
	return h, err
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

