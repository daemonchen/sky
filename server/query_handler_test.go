package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that we can query the server for a count of events.
func TestServerSimpleCountQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "fruit", true, "factor")
		setupTestProperty("foo", "num", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"a0", "2012-01-01T00:00:00Z", `{"data":{"fruit":"apple"}}`},
			[]string{"a1", "2012-01-01T00:00:00Z", `{"data":{"fruit":"grape"}}`},
			[]string{"a1", "2012-01-01T00:00:01Z", `{"data":{"num":12}}`},
			[]string{"a2", "2012-01-01T00:00:00Z", `{"data":{"fruit":"orange"}}`},
			[]string{"a3", "2012-01-01T00:00:00Z", `{"data":{"fruit":"apple"}}`},
		})

		setupTestTable("bar")
		setupTestProperty("bar", "fruit", true, "factor")
		setupTestData(t, "bar", [][]string{
			[]string{"xx", "2012-01-01T00:00:00Z", `{"data":{"fruit":"grape"}}`},
		})

		// Run queries against 'foo' and 'bar'.
		q := `SELECT count()`

		status, resp := postJSON("/tables/foo/query?prefix=", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, 200, status)
		assert.Equal(t, jsonenc(resp), `{"count":5}`)

		status, resp = postJSON("/tables/bar/query?prefix=", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, 200, status)
		assert.Equal(t, jsonenc(resp), `{"count":1}`)
	})
}

// Ensure that we can query the server for a count of events with a single dimension.
func TestServerOneDimensionCountQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "fruit", true, "factor")
		setupTestProperty("foo", "num", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"b0", "2012-01-01T00:00:00Z", `{"data":{"fruit":"apple"}}`},
			[]string{"b1", "2012-01-01T00:00:00Z", `{"data":{"fruit":"grape"}}`},
			[]string{"b1", "2012-01-01T00:00:01Z", `{"data":{"num":12}}`},
			[]string{"b2", "2012-01-01T00:00:00Z", `{"data":{"fruit":"orange"}}`},
			[]string{"b3", "2012-01-01T00:00:00Z", `{"data":{"fruit":"apple"}}`},
		})

		// Run query.
		q := `SELECT count() AS count GROUP BY fruit`
		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, jsonenc(resp), `{"fruit":{"":{"count":1},"apple":{"count":2},"grape":{"count":1},"orange":{"count":1}}}`)
	})
}

// Ensure that we can query the server for multiple selections with multiple dimensions.
func TestServerMultiDimensionalQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "gender", false, "factor")
		setupTestProperty("foo", "state", false, "factor")
		setupTestProperty("foo", "price", true, "float")
		setupTestProperty("foo", "num", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"c0", "2012-01-01T00:00:00Z", `{"data":{"gender":"m", "state":"NY", "price":100}}`},
			[]string{"c0", "2012-01-01T00:00:01Z", `{"data":{"price":200}}`},
			[]string{"c0", "2012-01-01T00:00:02Z", `{"data":{"state":"CA","price":10}}`},

			[]string{"c1", "2012-01-01T00:00:00Z", `{"data":{"gender":"m", "state":"CA", "price":20}}`},
			[]string{"c1", "2012-01-01T00:00:01Z", `{"data":{"num":1000}}`},

			[]string{"c2", "2012-01-01T00:00:00Z", `{"data":{"gender":"f", "state":"NY", "price":30}}`},
		})

		// Run query.
		q := `
			SELECT sum((price + num) * 2) AS sum INTO "X"
			SELECT count() AS count, sum(price) AS sum GROUP BY gender, state INTO "s1"
		`
		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, jsonenc(resp), `{"X":{"sum":2720},"s1":{"gender":{"f":{"state":{"NY":{"count":1,"sum":30}}},"m":{"state":{"CA":{"count":3,"sum":30},"NY":{"count":2,"sum":300}}}}}}`)
	})
}

// Ensure that we can perform a non-sessionized funnel analysis.
func TestServerFunnelAnalysisQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "action", true, "factor")
		setupTestData(t, "foo", [][]string{
			// A0[0..0]..A1[1..2] occurs twice for this object.
			[]string{"d0", "2012-01-01T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"d0", "2012-01-01T00:00:01Z", `{"data":{"action":"A1"}}`},
			[]string{"d0", "2012-01-01T00:00:02Z", `{"data":{"action":"A2"}}`},
			[]string{"d0", "2012-01-01T12:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"d0", "2012-01-01T13:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"d0", "2012-01-01T14:00:00Z", `{"data":{"action":"A1"}}`},

			// A0[0..0]..A1[1..2] occurs once for this object. (Second time matches A1[1..3]).
			[]string{"e1", "2012-01-01T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"e1", "2012-01-01T00:00:01Z", `{"data":{"action":"A0"}}`},
			[]string{"e1", "2012-01-01T00:00:02Z", `{"data":{"action":"A1"}}`},
			[]string{"e1", "2012-01-02T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"e1", "2012-01-02T00:00:01Z", `{"data":{"action":"A0"}}`},
			[]string{"e1", "2012-01-02T00:00:02Z", `{"data":{"action":"A0"}}`},
			[]string{"e1", "2012-01-02T00:00:03Z", `{"data":{"action":"A1"}}`},
		})

		// Run query.
		q := `
			WHEN action == 'A0' THEN
				WHEN action == 'A1' WITHIN 1..2 STEPS THEN
					SELECT count() AS count GROUP BY action
				END
			END
		`

		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, jsonenc(resp), `{"action":{"A1":{"count":3}}}`)
	})
}

// Ensure that we can factorize overlapping factors.
func TestServerFactorizeOverlappingQueries(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "action", false, "factor")
		setupTestData(t, "foo", [][]string{
			[]string{"f0", "2012-01-01T00:00:00Z", `{"data":{"action":"A0"}}`},
		})

		// Run query.
		q := `
			SELECT count() AS count1 GROUP BY action INTO "q"
			SELECT count() AS count2 GROUP BY action INTO "q"
		`

		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, jsonenc(resp), `{"q":{"action":{"A0":{"count1":1,"count2":1}}}}`)
	})
}

// Ensure that we can perform a sessionized funnel analysis.
func TestServerSessionizedFunnelAnalysisQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "action", false, "factor")
		setupTestData(t, "foo", [][]string{
			// A0[0..0]..A1[1..1] occurs once for this object. The second one is broken across sessions.
			[]string{"f0", "1970-01-01T00:00:01Z", `{"data":{"action":"A0"}}`},
			[]string{"f0", "1970-01-01T01:59:59Z", `{"data":{"action":"A1"}}`},
			[]string{"f0", "1970-01-02T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"f0", "1970-01-02T02:00:00Z", `{"data":{"action":"A1"}}`},
		})

		// Run query.
		q := `
			FOR EACH SESSION DELIMITED BY 2 HOURS
				FOR EACH EVENT
					WHEN action == "A0" THEN
						WHEN action == "A1" WITHIN 1..1 STEPS THEN
							SELECT count() AS count GROUP BY action
						END
					END
				END
			END
		`

		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, jsonenc(resp), `{"action":{"A1":{"count":1}}}`)
	})
}

// Ensure that we can access system variables like eos and eof.
func TestServerSystemVariables(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "action", false, "factor")
		setupTestData(t, "foo", [][]string{
			[]string{"f0", "1970-01-01T00:00:01Z", `{"data":{"action":"A0"}}`},
			[]string{"f0", "1970-01-01T01:59:59Z", `{"data":{"action":"A1"}}`},
			[]string{"f0", "1970-01-02T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"f0", "1970-01-02T02:00:00Z", `{"data":{"action":"A1"}}`},

			[]string{"f1", "1970-01-01T02:00:00Z", `{"data":{"action":"A0"}}`},
		})

		q := `
			FOR EACH SESSION DELIMITED BY 2 HOURS
				FOR EACH EVENT
					SELECT count() AS count GROUP BY action, @@eos, @@eof
				END
			END
		`

		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, jsonenc(resp), `{"action":{"A0":{"@eos":{"false":{"@eof":{"false":{"count":1}}},"true":{"@eof":{"false":{"count":1},"true":{"count":1}}}}},"A1":{"@eos":{"true":{"@eof":{"false":{"count":1},"true":{"count":1}}}}}}}`)
	})
}

// Ensure that we can utilitize the timestamp in the query.
func TestServerTimestampQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "action", true, "factor")
		setupTestData(t, "foo", [][]string{
			[]string{"00", "1970-01-01T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"00", "1970-01-01T00:00:02Z", `{"data":{"action":"A1"}}`},
			[]string{"00", "1970-01-01T00:00:04Z", `{"data":{"action":"A2"}}`},
			[]string{"00", "1970-01-01T00:00:06Z", `{"data":{"action":"A3"}}`},
			[]string{"00", "1970-01-01T00:01:00Z", `{"data":{"action":"A4"}}`},
			[]string{"01", "1970-01-01T00:00:02Z", `{"data":{"action":"A5"}}`},
			[]string{"02", "1970-01-01T00:00:02Z", `{"data":{"action":"A5"}}`},
		})

		// Run query.
		q := `
			WHEN @@timestamp >= 2 && @@timestamp < 6 THEN
				SELECT count() AS count, sum(@@timestamp) AS tsSum GROUP BY action
			END
		`

		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, jsonenc(resp), `{"action":{"A1":{"count":1,"tsSum":2},"A2":{"count":1,"tsSum":4},"A5":{"count":2,"tsSum":4}}}`)
	})
}

// Ensure that we can use declared variables.
func TestServerFSMQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "action", true, "factor")
		setupTestData(t, "foo", [][]string{
			[]string{"00", "1970-01-01T00:00:00Z", `{"data":{"action":"home"}}`},
			[]string{"00", "1970-01-01T00:00:02Z", `{"data":{"action":"signup"}}`},
			[]string{"00", "1970-01-01T00:00:03Z", `{"data":{"action":"signed_up"}}`},
			[]string{"00", "1970-01-01T00:00:04Z", `{"data":{"action":"pricing"}}`},
			[]string{"00", "1970-01-02T00:00:00Z", `{"data":{"action":"cancel"}}`},
			[]string{"00", "1970-01-03T00:00:00Z", `{"data":{"action":"home"}}`},

			[]string{"01", "1970-01-01T00:00:00Z", `{"data":{"action":"home"}}`},
			[]string{"01", "1970-01-01T00:00:02Z", `{"data":{"action":"cancel"}}`},
		})

		// Run query.
		q := `
			DECLARE state AS INTEGER
			DECLARE bool AS BOOLEAN
			SET bool = true
			WHEN state == 0 THEN
				SET state = 1
				SELECT count() AS count INTO "visited"
			END
			WHEN state == 1 && action == "signed_up" THEN
				SET state = 2
				SELECT count() AS count INTO "registered"
			END
			WHEN state == 2 && action == "cancel" THEN
				SET state = 3
				SELECT count() AS count INTO "cancelled"
			END
		`

		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, jsonenc(resp), `{"cancelled":{"count":1},"registered":{"count":1},"visited":{"count":2}}`)
	})
}

// Ensure that we can use variables associated with factor properties.
func TestServerFactorVariableQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "action", true, "factor")
		setupTestData(t, "foo", [][]string{
			[]string{"00", "1970-01-01T00:00:00Z", `{"data":{"action":"x"}}`},
			[]string{"00", "1970-01-01T00:00:02Z", `{"data":{"action":"y"}}`},
			[]string{"00", "1970-01-01T00:00:03Z", `{"data":{"action":"z"}}`},

			[]string{"01", "1970-01-01T00:00:00Z", `{"data":{"action":"y"}}`},
			[]string{"01", "1970-01-01T00:00:02Z", `{"data":{"action":"z"}}`},
		})

		// Run query.
		q := `
			DECLARE prev_action AS FACTOR(action)
			WHEN prev_action != "x" THEN
				SELECT count() AS count GROUP BY prev_action, action
			END
			SET prev_action = action
		`

		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, jsonenc(resp), `{"prev_action":{"":{"action":{"x":{"count":1},"y":{"count":1}}},"y":{"action":{"z":{"count":2}}}}}`)
	})
}

// Ensure that we can can filter by prefix.
func TestServerPrefixQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "price", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"00000", "2012-01-01T00:00:00Z", `{"data":{"price":1000}}`},
			[]string{"0010a", "2012-01-01T00:00:00Z", `{"data":{"price":100}}`},
			[]string{"0010b", "2012-01-01T00:00:00Z", `{"data":{"price":200}}`},
			[]string{"0010b", "2012-01-01T00:00:01Z", `{"data":{"price":50}}`},
			[]string{"0020a", "2012-01-01T00:00:00Z", `{"data":{"price":30}}`},
			[]string{"0030a", "2012-01-01T00:00:00Z", `{"data":{"price":40}}`},
		})

		// Run query.
		q := `SELECT sum(price) AS totalPrice`

		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"prefix": "001", "query": q}))
		assert.Equal(t, jsonenc(resp), `{"totalPrice":350}`)
	})
}

// Ensure that we can can query with raw text.
func TestServerRawQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "price", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"x", "2012-01-01T00:00:00Z", `{"data":{"price":100}}`},
		})
		q := `SELECT sum(price) AS totalPrice`
		_, resp := sendText("POST", "/tables/foo/query", q)
		assert.Equal(t, resp, `{"totalPrice":100}`+"\n")
	})
}

// Ensure that we can select non-aggregated field with a selection.
/*
func TestServerExportQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "val", true, "integer")
		setupTestProperty("foo", "my_string", true, "string")
		setupTestProperty("foo", "my_factor", true, "factor")
		setupTestProperty("foo", "my_integer", true, "integer")
		setupTestProperty("foo", "my_float", true, "float")
		setupTestProperty("foo", "my_boolean", true, "boolean")
		setupTestData(t, "foo", [][]string{
			[]string{"0001", "2012-01-01T00:00:00Z", `{"data":{"my_string":"xxx", "my_factor":"yyy", "my_integer":100, "my_float":10.2, "my_boolean":true}}`},
			[]string{"0001", "2012-01-01T00:00:01Z", `{"data":{"val":1000}}`},
		})
		q := `
			SELECT my_factor INTO "named"
			SELECT my_factor INTO "named"
			SELECT my_factor AS factor2 GROUP BY my_integer
			SELECT count() GROUP BY my_integer
			SELECT timestamp, my_string, my_factor, my_integer, my_float, my_boolean
		`
		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, jsonenc(resp), `{"_":[{"my_boolean":true,"my_factor":"yyy","my_float":10.2,"my_integer":100,"my_string":"xxx","timestamp":1325376000},{"my_boolean":false,"my_factor":"","my_float":0,"my_integer":0,"my_string":"","timestamp":1325376001}],"my_integer":{"0":{"_":[{"factor2":""}],"count":1},"100":{"_":[{"factor2":"yyy"}],"count":1}},"named":{"_":[{"my_factor":"yyy"},{"my_factor":"yyy"},{"my_factor":""},{"my_factor":""}]}}`)
	})
}
*/

// Ensure that we can select into the same field but dedupe merges.
func TestServerDuplicateSelectionQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "value", true, "integer")
		setupTestData(t, "foo", [][]string{[]string{"x", "2012-01-01T00:00:00Z", `{"data":{"value":100}}`}})
		q := `
			SELECT count() AS count GROUP BY value
			SELECT count() AS count GROUP BY value
		`
		_, resp := postJSON("/tables/foo/query", jsonenc(map[string]interface{}{"query": q}))
		assert.Equal(t, jsonenc(resp), `{"value":{"100":{"count":2}}}`)
	})
}

// Ensure that we can run basic stats.
func TestServerQueryCount(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "price", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"0", "1970-01-01T00:00:00Z", `{"data":{"price":1000}}`},
			[]string{"0010a", "2012-01-01T00:00:00Z", `{"data":{"price":100}}`},
			[]string{"0010b", "2012-01-01T00:00:00Z", `{"data":{"price":200}}`},
			[]string{"0010b", "2012-01-01T00:00:01Z", `{"data":{"price":0}}`},
			[]string{"0020a", "2012-01-01T00:00:00Z", `{"data":{"price":30}}`},
			[]string{"0030a", "2012-01-01T00:00:00Z", `{"data":{"price":40}}`},
		})

		code, resp := getJSON("/tables/foo/count?prefix=001")
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `{"count":3}`)
	})
}

func TestServerQueryFixture0001(t *testing.T) { testQueryFixture(t, "0001") }

// Loads the "input", executes the "query", and checks it agains the "output".
func testQueryFixture(t *testing.T, name string) {
	runTestServer(func(s *Server) {
		re := regexp.MustCompile(`^(\S+)\s+(\S+)\s+(.+)$`)

		// Read and process input file.
		f, err := os.Open(filepath.Join("fixtures", name, "input"))
		if err != nil {
			panic("fixtures error: " + err.Error())
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if len(line) == 0 || strings.HasPrefix(line, "#") {
				continue
			}
			m := re.FindStringSubmatch(line)
			if m == nil || len(m) != 4 {
				panic("invalid fixture input: " + line)
			}
			method, path, body := m[1], m[2], m[3]
			if statusCode, ret := sendJSON(method, path, body); statusCode != 200 {
				panic(fmt.Sprintf("invalid fixture status code: %s %s: %d: %s", method, path, statusCode, jsonenc(ret)))
			}
		}
		if err := scanner.Err(); err != nil {
			panic("scanner error: " + err.Error())
		}
		f.Close()

		// Execute the query.
		q, err := ioutil.ReadFile(filepath.Join("fixtures", name, "query"))
		if err != nil {
			panic("fixture query error: " + err.Error())
		}
		status, resp := postJSON("/tables/tbl/query", jsonenc(map[string]interface{}{"query": string(q)}))
		assert.Equal(t, 200, status)

		// Verify the output.
		exp, err := ioutil.ReadFile(filepath.Join("fixtures", name, "output"))
		if err != nil {
			panic("fixture output error: " + err.Error())
		}
		actual, _ := json.MarshalIndent(resp, "", "  ")
		assert.Equal(t, strings.TrimSpace(string(exp)), strings.TrimSpace(string(actual)))
	})
}
