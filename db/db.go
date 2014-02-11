package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/skydb/sky/hash"
)

// DB represents Sky's file-back data store.
type DB struct {
	sync.RWMutex
	NoSync     bool
	MaxDBs     uint
	MaxReaders uint

	defaultShardCount int
	factorizers       map[string]*Factorizer
	path              string
	shards            []*shard
}

func (db *DB) Path() string {
	return db.path
}

func (db *DB) dataPath() string {
	return filepath.Join(db.path, "data")
}

func (db *DB) factorsPath() string {
	return filepath.Join(db.path, "factors")
}

func (db *DB) shardPath(index int) string {
	return filepath.Join(db.dataPath(), strconv.Itoa(index))
}

// Opens the database.
func (db *DB) Open(path string, shardCount int) error {
	db.Lock()
	defer db.Unlock()

	// Initialize poperties.
	db.path = path
	db.factorizers = make(map[string]*Factorizer)

	// Create directory if it doesn't exist.
	if err := os.MkdirAll(db.dataPath(), 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(db.factorsPath(), 0700); err != nil {
		return err
	}

	// If database already exists then use the existing number of shards.
	n, err := db.shardCount()
	if err != nil {
		return err
	} else if n > 0 {
		shardCount = n
	} else if shardCount == 0 {
		shardCount = runtime.NumCPU()
	}

	// Create and open each shard.
	db.shards = make([]*shard, 0)
	for i := 0; i < shardCount; i++ {
		shard := &shard{
			maxDBs:     db.MaxDBs,
			maxReaders: db.MaxReaders,
			noSync:     db.NoSync,
		}

		db.shards = append(db.shards, shard)
		if err := shard.Open(db.shardPath(i)); err != nil {
			db.close()
			return err
		}
	}

	return nil
}

// Close shuts down all open database resources.
func (db *DB) Close() {
	db.Lock()
	defer db.Unlock()
	db.close()
}

func (db *DB) close() {
	// Close factorizers.
	for _, f := range db.factorizers {
		f.Close()
	}
	db.factorizers = nil

	// Close shards.
	for _, s := range db.shards {
		s.Close()
	}
	db.shards = nil

	db.path = ""
}

// getShardByObjectId retrieves the appropriate shard for a given object identifier.
func (db *DB) getShardByObjectId(id string) *shard {
	index := hash.Local(id) % uint32(len(db.shards))
	return db.shards[index]
}

// shardCount retrieves the number of shards in the database. This is determined
// by the number of numeric directories in the data path. If no directories exist
// then a default count is used.
func (db *DB) shardCount() (int, error) {
	infos, err := ioutil.ReadDir(db.dataPath())
	if err != nil {
		return 0, err
	}

	count := 0
	for _, info := range infos {
		index, err := strconv.Atoi(info.Name())
		if info.IsDir() && err == nil && (index+1) > count {
			count = index + 1
		}
	}

	if count == 0 {
		count = db.defaultShardCount
	}

	return count, nil
}

// Factorizer returns a table's factorizer.
func (db *DB) Factorizer(tablespace string) (*Factorizer, error) {
	db.Lock()
	defer db.Unlock()
	return db.factorizer(tablespace)
}

func (db *DB) factorizer(tablespace string) (*Factorizer, error) {
	// Retrieve already open factorizer if available.
	if f := db.factorizers[tablespace]; f != nil {
		return f, nil
	}

	// Otherwise create a new factorizer for the table.
	f := NewFactorizer()
	f.NoSync = db.NoSync
	f.MaxDBs = db.MaxDBs
	f.MaxReaders = db.MaxReaders

	path := filepath.Join(db.factorsPath(), tablespace)
	if err := f.Open(path); err != nil {
		return nil, err
	}

	// Save the open factorizer to the lookup.
	db.factorizers[tablespace] = f

	return f, nil
}

// Cursors retrieves a set of cursors for iterating over the database.
func (db *DB) Cursors(tablespace string) (Cursors, error) {
	cursors := make(Cursors, 0)
	for _, s := range db.shards {
		c, err := s.Cursor(tablespace)
		if err != nil {
			cursors.Close()
			return nil, fmt.Errorf("db cursors error: %s", err)
		}
		cursors = append(cursors, c)
	}
	return cursors, nil
}

func (db *DB) GetEvent(tablespace string, id string, timestamp time.Time) (*Event, error) {
	s := db.getShardByObjectId(id)
	return s.GetEvent(tablespace, id, timestamp)
}

func (db *DB) GetEvents(tablespace string, id string) ([]*Event, error) {
	s := db.getShardByObjectId(id)
	return s.GetEvents(tablespace, id)
}

// InsertEvent adds a single event to the database.
func (db *DB) InsertEvent(tablespace string, id string, event *Event) error {
	s := db.getShardByObjectId(id)
	return s.InsertEvent(tablespace, id, event)
}

// InsertEvents adds multiple events for a single object.
func (db *DB) InsertEvents(tablespace string, id string, newEvents []*Event) error {
	s := db.getShardByObjectId(id)
	return s.InsertEvents(tablespace, id, newEvents)
}

// InsertObjects bulk inserts events for multiple objects.
func (db *DB) InsertObjects(tablespace string, objects map[string][]*Event) (int, error) {
	count := 0
	for id, events := range objects {
		s := db.getShardByObjectId(id)
		if err := s.InsertEvents(tablespace, id, events); err != nil {
			return count, err
		}
		count += len(events)
	}
	return count, nil
}

func (db *DB) DeleteEvent(tablespace string, id string, timestamp time.Time) error {
	s := db.getShardByObjectId(id)
	return s.DeleteEvent(tablespace, id, timestamp)
}

func (db *DB) DeleteObject(tablespace string, id string) error {
	s := db.getShardByObjectId(id)
	return s.DeleteObject(tablespace, id)
}

func (db *DB) Merge(tablespace string, destinationId string, sourceId string) error {
	dest := db.getShardByObjectId(destinationId)
	src := db.getShardByObjectId(sourceId)

	// Retrieve source events.
	srcEvents, err := src.GetEvents(tablespace, sourceId)
	if err != nil {
		return err
	}

	// Insert events into destination object.
	if len(srcEvents) > 0 {
		if err = dest.InsertEvents(tablespace, destinationId, srcEvents); err != nil {
			return err
		}
		if err = src.DeleteObject(tablespace, sourceId); err != nil {
			return err
		}
	}

	return nil
}

// Drop removes a table from the database.
func (db *DB) Drop(tablespace string) error {
	var err error
	for _, s := range db.shards {
		if _err := s.Drop(tablespace); err == nil {
			err = _err
		}
	}
	return err
}

func (db *DB) Stats() ([]*Stat, error) {
	stats := make([]*Stat, 0, len(db.shards))
	for _, shard := range db.shards {
		stat, err := shard.Stat()
		if err != nil {
			return stats, err
		}
		stats = append(stats, stat)
	}
	return stats, nil
}
