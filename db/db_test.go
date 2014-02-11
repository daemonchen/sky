package db

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/szferi/gomdb"
)

func TestDBOpen(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	db := &DB{}
	assert.NoError(t, db.Open(path, 2))
	assert.Equal(t, path, db.Path())
	assert.Equal(t, len(db.shards), 2)
	assert.Equal(t, filepath.Join(path, "data"), db.dataPath())
	assert.Equal(t, filepath.Join(path, "data", "2"), db.shardPath(2))
	assert.Equal(t, filepath.Join(path, "factors"), db.factorsPath())
	db.Close()
}

func TestDBInsertEvent(t *testing.T) {
	withDB(0, func(db *DB) {
		assert.NoError(t, db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john")))
		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Equal(t, e.Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, e.Data[1], "john", "")
	})
}

func TestDBInsertEvents(t *testing.T) {
	withDB(0, func(db *DB) {
		input := []*Event{
			testevent("2000-01-01T00:00:02Z", 2, 100),
			testevent("2000-01-01T00:00:00Z", 1, "john"),
		}
		db.InsertEvents("foo", "bar", input)
		events, err := db.GetEvents("foo", "bar")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 2, "")
		assert.Equal(t, events[0].Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, events[0].Data[1], "john", "")
		assert.Equal(t, events[1].Timestamp, musttime("2000-01-01T00:00:02Z"), "")
		assert.Equal(t, events[1].Data[2], 100, "")
	})
}

func TestDBInsertObjects(t *testing.T) {
	withDB(0, func(db *DB) {
		input := map[string][]*Event{
			"bar": []*Event{
				testevent("2000-01-01T00:00:02Z", 2, 100),
				testevent("2000-01-01T00:00:00Z", 1, "john"),
			},
			"bat": []*Event{
				testevent("2000-01-01T00:00:00Z", 1, "jose"),
			},
		}

		n, err := db.InsertObjects("foo", input)
		assert.Nil(t, err, "")
		assert.Equal(t, n, 3, "")

		events, err := db.GetEvents("foo", "bar")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 2, "")
		assert.Equal(t, events[0].Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, events[0].Data[1], "john", "")
		assert.Equal(t, events[1].Timestamp, musttime("2000-01-01T00:00:02Z"), "")
		assert.Equal(t, events[1].Data[2], 100, "")

		events, err = db.GetEvents("foo", "bat")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 1, "")
		assert.Equal(t, events[0].Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, events[0].Data[1], "jose", "")
	})
}

func TestDBInsertNonSequentialEvents(t *testing.T) {
	withDB(0, func(db *DB) {
		db.InsertEvent("foo", "bar", testevent("2000-01-02T00:00:00Z", 1, "john", -1, 100))
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "jane", 2, "test"))
		db.InsertEvent("foo", "bar", testevent("2000-01-03T00:00:00Z", 1, "jose"))
		events, err := db.GetEvents("foo", "bar")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 3, "")
		assert.Equal(t, events[0].Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, events[0].Data[-1], nil, "")
		assert.Equal(t, events[0].Data[1], "jane", "")
		assert.Equal(t, events[0].Data[2], "test", "")
		assert.Equal(t, events[1].Timestamp, musttime("2000-01-02T00:00:00Z"), "")
		assert.Equal(t, events[1].Data[-1], 100, "")
		assert.Equal(t, events[1].Data[1], "john", "")
		assert.Equal(t, events[1].Data[2], nil, "")
		assert.Equal(t, events[2].Timestamp, musttime("2000-01-03T00:00:00Z"), "")
		assert.Equal(t, events[2].Data[-1], nil, "")
		assert.Equal(t, events[2].Data[1], "jose", "")
		assert.Equal(t, events[2].Data[2], nil, "")
	})
}

func TestDBDeleteEvent(t *testing.T) {
	withDB(0, func(db *DB) {
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"))
		db.DeleteEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Nil(t, e, "")
	})
}

func TestDBDeleteMissingEvent(t *testing.T) {
	withDB(0, func(db *DB) {
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"))
		db.DeleteEvent("foo", "bar", musttime("2000-01-02T00:00:00Z"))
		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.NotNil(t, e, "")
	})
}

func TestDBDeleteObject(t *testing.T) {
	withDB(0, func(db *DB) {
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"))
		db.InsertEvent("foo", "bar", testevent("2000-01-02T00:00:00Z", 1, "jane"))
		db.DeleteObject("foo", "bar")
		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Nil(t, e, "")
	})
}

func TestDBMerge(t *testing.T) {
	withDB(0, func(db *DB) {
		db.InsertEvent("foo", "bar", testevent("2000-01-03T00:00:00Z", 1, "john"))
		db.InsertEvent("foo", "bar", testevent("2000-01-02T00:00:00Z", 1, "jane"))
		db.InsertEvent("foo", "bat", testevent("2000-01-02T00:00:00Z", 1, "joe"))
		db.InsertEvent("foo", "bat", testevent("2000-01-01T00:00:00Z", 1, "jose"))
		err := db.Merge("foo", "bar", "bat")
		events, err := db.GetEvents("foo", "bar")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 3, "")
		assert.Equal(t, events[0].Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, events[0].Data[1], "jose", "")
		assert.Equal(t, events[1].Timestamp, musttime("2000-01-02T00:00:00Z"), "")
		assert.Equal(t, events[1].Data[1], "joe", "")
		assert.Equal(t, events[2].Timestamp, musttime("2000-01-03T00:00:00Z"), "")
		assert.Equal(t, events[2].Data[1], "john", "")
		events, err = db.GetEvents("foo", "bat")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 0, "")
	})
}

func TestDBReopen(t *testing.T) {
	withDB(2, func(db *DB) {
		path := db.Path()

		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"))
		db.Close()
		assert.Equal(t, "", db.Path())

		err := db.Open(path, 0)
		assert.Nil(t, err, "")
		assert.Equal(t, len(db.shards), 2, "")

		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Equal(t, e.Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, e.Data[1], "john", "")
	})
}

func TestDBCursors(t *testing.T) {
	withDB(0, func(db *DB) {
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"))
		db.InsertEvent("foo", "baz", testevent("2000-01-01T00:00:00Z", 1, "john"))
		cursors, err := db.Cursors("foo")
		defer cursors.Close()
		assert.Nil(t, err, "")

		keys := make([]string, 0)
		for _, c := range cursors {
			for {
				key, _, err := c.Get(nil, mdb.NEXT)
				if err == mdb.NotFound {
					break
				}
				assert.Nil(t, err, "")
				keys = append(keys, string(key))
			}
		}
		sort.Strings(keys)
		assert.Equal(t, keys, []string{"bar", "baz"}, "")
	})
}

func TestDBStats(t *testing.T) {
	withDB(0, func(db *DB) {
		count, err := db.shardCount()
		assert.Nil(t, err, "")
		stats, err := db.Stats()
		assert.Nil(t, err, "")
		assert.Equal(t, len(stats), count)
	})
}

func TestDBDrop(t *testing.T) {
	withDB(0, func(db *DB) {
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"))
		db.Drop("foo")
		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Nil(t, e, "")
	})
}

func withDB(shardCount int, f func(db *DB)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	db := &DB{}
	if err := db.Open(path, shardCount); err != nil {
		panic(err.Error())
	}
	defer db.Close()

	f(db)
}

func testevent(timestamp string, args ...interface{}) *Event {
	e := &Event{Timestamp: musttime(timestamp)}
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
