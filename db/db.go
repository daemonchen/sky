package db

import (
	"os"
	"path/filepath"
	"sync"
)

const (
	// DefaultMaxDBs is the default limit of databases Sky can use.
	DefaultMaxDBs = uint(1024)

	// DefaultMaxReaders is the default limit of readers that Sky can use.
	DefaultMaxReaders = uint(126)
)

// DB represents Sky's file-backed data store.
type DB struct {
	sync.RWMutex
	NoSync     bool
	MaxDBs     uint
	MaxReaders uint

	tables map[string]*Table
	path   string
}

// Path returns the root path of the database.
func (db *DB) Path() string {
	return db.path
}

// tablePath returns the path of a named table.
func (db *DB) tablePath(name string) string {
	return filepath.Join(db.path, name)
}

// Open opens and initializes the database.
func (db *DB) Open(path string) error {
	db.Lock()
	defer db.Unlock()

	// Return an error if the database is currently opened.
	if db.path != "" {
		return ErrDatabaseOpen
	}

	// Initialize poperties.
	db.path = path
	db.tables = make(map[string]*Table)

	// Create directory if it doesn't exist.
	if err := os.MkdirAll(db.path, 0700); err != nil {
		return err
	}

	return nil
}

// Close closes all tables.
func (db *DB) Close() {
	db.Lock()
	defer db.Unlock()
	db.close()
}

func (db *DB) close() {
	for _, t := range db.tables {
		t.close()
	}
	db.tables = nil
	db.path = ""
}

// CreateTable creates, opens and initializes a table.
// Returns an error if the table already exists.
func (db *DB) CreateTable(name string) (*Table, error) {
	db.Lock()
	defer db.Unlock()

	if _, err := db.openTable(name); err == nil {
		return nil, ErrTableExists
	} else if err != ErrTableNotFound {
		return nil, err
	}

	// Create table.
	t := db.table(name)
	if err := t.create(); err != nil {
		return nil, err
	}

	// Return opened table.
	return db.openTable(name)
}

// DropTable closes and removes a table from the database.
// Returns an error if the database does not exist.
func (db *DB) DropTable(name string) error {
	db.Lock()
	defer db.Unlock()
	if db.path == "" {
		return ErrDatabaseNotOpen
	}

	// Find the table.
	t := db.table(name)
	if !t.Exists() {
		return ErrTableNotFound
	}

	// Drop table and remove it from the cache.
	if err := t.drop(); err != nil {
		return err
	}
	delete(db.tables, name)

	return nil
}

// OpenTable opens and initializes a table.
// Returns an error if the table does not exist.
func (db *DB) OpenTable(name string) (*Table, error) {
	db.Lock()
	defer db.Unlock()
	return db.openTable(name)
}

func (db *DB) openTable(name string) (*Table, error) {
	if db.path == "" {
		return nil, ErrDatabaseNotOpen
	} else if name == "" {
		return nil, ErrTableNameRequired
	}

	t := db.table(name)
	if err := t.open(); err != nil {
		return nil, err
	}

	// Cache the table and return it.
	db.tables[name] = t
	return t, nil
}

// table creates a reference to a table associated with this database.
// Returns a reference to an existing table if one already exists.
func (db *DB) table(name string) *Table {
	// Check for an already open table.
	if t, ok := db.tables[name]; ok {
		return t
	}

	// Set defaults on table.
	maxDBs := db.MaxDBs
	maxReaders := db.MaxReaders
	if maxDBs == 0 {
		maxDBs = DefaultMaxDBs
	}
	if maxReaders == 0 {
		maxReaders = DefaultMaxReaders
	}

	// Create table instance and return it.
	return &Table{
		db:         db,
		name:       name,
		path:       db.tablePath(name),
		noSync:     db.NoSync,
		maxDBs:     maxDBs,
		maxReaders: maxReaders,
	}
}
