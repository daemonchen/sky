package test

import (
	"testing"

	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query/ast"
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
