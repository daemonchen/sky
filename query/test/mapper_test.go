package test

import (
	"testing"

	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
	"github.com/skydb/sky/query/codegen/hashmap"
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
